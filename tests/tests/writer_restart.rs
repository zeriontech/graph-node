use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use graph::env::EnvVars;
use graph::prelude::diesel::connection::SimpleConnection;
use graph::prelude::diesel::dsl::sql;
use graph::prelude::diesel::sql_types::{BigInt, Integer, Text};
use graph::prelude::diesel::{self, Connection, PgConnection, RunQueryDsl};
use graph::prelude::{BlockNumber, BlockPtr, SubgraphStore as _};
use graph_tests::fixture::ethereum::{chain, empty_block, genesis, push_test_log};
use graph_tests::fixture::{self, TestContext, test_ptr};
use graph_tests::recipe::RunnerTestRecipe;
use tokio::sync::Mutex;

static ONE_TEST_AT_A_TIME: Mutex<()> = Mutex::const_new(());

/// Writes every block in its own transaction and lets at most one write
/// wait in the queue, so a failed write is reported to the runner while it
/// processes the blocks that follow. The PoI stays out of the cross-block
/// entity cache: a failed writer drops the blocks queued behind the failed
/// one, and a cached PoI from a dropped block trips the entity cache's
/// debug-build consistency check against the store.
fn configure_writer_for_injected_failures() {
    unsafe {
        std::env::set_var("GRAPH_STORE_WRITE_QUEUE", "1");
        std::env::set_var("GRAPH_STORE_WRITE_BATCH_DURATION", "0");
        std::env::set_var("GRAPH_ENTITY_CACHE_EXCLUDE", "Poi$");
    }
}

fn db() -> PgConnection {
    let url = std::env::var("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("THEGRAPH_STORE_POSTGRES_DIESEL_URL must be set");
    PgConnection::establish(&url).expect("connect to the test database")
}

fn deployment_namespace(ctx: &TestContext) -> String {
    let id = ctx.deployment.id;
    diesel::select(sql::<Text>(&format!(
        "(select name from deployment_schemas where id = {id})"
    )))
    .get_result(&mut db())
    .unwrap()
}

/// Fails the first transaction that moves the deployment head to `block` or
/// later; the sequence is not rolled back with it, so later writes succeed.
/// Returns the deployment's namespace.
fn fail_first_write_reaching(ctx: &TestContext, block: BlockNumber) -> String {
    let id = ctx.deployment.id;
    let nsp = deployment_namespace(ctx);
    db().batch_execute(&format!(
        "
        create sequence {nsp}.failed_writes;

        create function {nsp}.fail_first_write() returns trigger
        language plpgsql as $$
        begin
            if nextval('{nsp}.failed_writes') = 1 then
                raise exception 'injected store error';
            end if;
            return new;
        end;
        $$;

        create trigger fail_first_write_{nsp}
            before update of block_number on subgraphs.head
            for each row
            when (new.id = {id} and new.block_number >= {block})
            execute function {nsp}.fail_first_write();
        "
    ))
    .unwrap();
    nsp
}

#[graph::test]
async fn poisoned_writer_is_replaced_on_restart() {
    let _one_test = ONE_TEST_AT_A_TIME.lock().await;
    configure_writer_for_injected_failures();

    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("poisoned_writer_is_replaced_on_restart", "end-block").await;

    let mut blocks = vec![genesis()];
    for number in 1..=5 {
        let mut block = empty_block(blocks.last().unwrap().ptr(), test_ptr(number));
        push_test_log(&mut block, number.to_string());
        blocks.push(block);
    }
    let stop_block = test_ptr(5);

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let mut env_vars = EnvVars::default();
    env_vars.subgraph_error_retry_ceil = Duration::from_secs(1);
    let ctx = fixture::setup(&test_info, &stores, &chain, None, Some(env_vars)).await;

    fail_first_write_reaching(&ctx, 2);

    let run = ctx.runner(stop_block.clone()).await.run_for_test(false);
    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the runner kept retrying with a poisoned writer")
        .unwrap();

    let head = ctx
        .store
        .least_block_ptr(&ctx.deployment.hash)
        .await
        .unwrap();
    assert_eq!(head, Some(stop_block));
    let status = ctx.indexing_status().await;
    assert!(status.fatal_error.is_none());
}

#[graph::test]
async fn data_source_restart_keeps_healthy_writer() {
    let _one_test = ONE_TEST_AT_A_TIME.lock().await;
    configure_writer_for_injected_failures();

    let RunnerTestRecipe { stores, test_info } = RunnerTestRecipe::new(
        "data_source_restart_keeps_healthy_writer",
        "data-source-revert2",
    )
    .await;

    let block_0 = genesis();
    let block_1 = empty_block(block_0.ptr(), test_ptr(1));
    let block_2 = empty_block(block_1.ptr(), test_ptr(2));
    let stop_block = block_2.ptr();

    let chain = chain(
        &test_info.test_name,
        vec![block_0, block_1, block_2],
        &stores,
        None,
    )
    .await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    let writable = || {
        ctx.store
            .clone()
            .writable(ctx.logger.clone(), ctx.deployment.id, Arc::new(vec![]))
    };
    let writer_before = writable().await.unwrap();

    let runner = ctx
        .runner(stop_block.clone())
        .await
        .run_for_test(false)
        .await
        .unwrap();

    // One static data source plus the template every block from 0 to 2 creates.
    assert_eq!(runner.context().hosts_len(), 4);
    let head = ctx
        .store
        .least_block_ptr(&ctx.deployment.hash)
        .await
        .unwrap();
    assert_eq!(head, Some(stop_block));
    let writer_after = writable().await.unwrap();
    assert!(std::ptr::addr_eq(
        Arc::as_ptr(&writer_before),
        Arc::as_ptr(&writer_after)
    ));
    assert!(writer_before.restart().await.unwrap().is_none());
}

/// From the write that `fail_first_write_reaching` fails until `end`, every
/// statement that reads `subgraphs.<table>` fails, as it would with Postgres
/// down; `table` keeps its rows behind a view while the outage lasts.
struct Outage {
    nsp: String,
    table: &'static str,
    ended: bool,
}

impl Outage {
    fn after_injected_failure(nsp: String, table: &'static str) -> Self {
        db().batch_execute(&format!(
            "
            create sequence {nsp}.outage_reads;

            create function {nsp}.database_reachable() returns boolean
            language plpgsql volatile as $$
            begin
                if (select is_called from {nsp}.failed_writes) then
                    perform nextval('{nsp}.outage_reads');
                    raise exception 'injected outage: database unavailable';
                end if;
                return true;
            end;
            $$;

            alter table subgraphs.{table} rename to {table}_behind_outage;
            create view subgraphs.{table} as
                select * from subgraphs.{table}_behind_outage
                 where {nsp}.database_reachable();
            "
        ))
        .unwrap();
        Outage {
            nsp,
            table,
            ended: false,
        }
    }

    fn failed_reads(&self) -> i64 {
        let nsp = &self.nsp;
        diesel::select(sql::<BigInt>(&format!(
            "(select case when is_called then last_value else 0 end from {nsp}.outage_reads)"
        )))
        .get_result(&mut db())
        .unwrap()
    }

    fn head_behind_outage(&self, ctx: &TestContext) -> BlockNumber {
        let id = ctx.deployment.id;
        diesel::select(sql::<Integer>(&format!(
            "(select block_number from subgraphs.head_behind_outage where id = {id})"
        )))
        .get_result(&mut db())
        .unwrap()
    }

    fn end(&mut self) {
        if !self.ended {
            let table = self.table;
            db().batch_execute(&format!(
                "
                drop view subgraphs.{table};
                alter table subgraphs.{table}_behind_outage rename to {table};
                "
            ))
            .unwrap();
            self.ended = true;
        }
    }
}

impl Drop for Outage {
    fn drop(&mut self) {
        self.end();
    }
}

/// Waits until the injected failures happened `failures` times, while the
/// runner is still running.
async fn wait_for_failures(count: impl Fn() -> i64, failures: i64, run: &mut RunningRunner) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while count() < failures {
        tokio::select! {
            result = &mut *run => panic!("the runner ended during the outage: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "failed {} times, waited for {failures}",
            count()
        );
    }
}

/// Fails every attempt to record a fatal error until `end`; the block writes
/// that carry the error succeed.
struct ErrorRecordingOutage {
    nsp: String,
    ended: bool,
}

impl ErrorRecordingOutage {
    fn start(ctx: &TestContext) -> Self {
        let id = ctx.deployment.id;
        let nsp = deployment_namespace(ctx);
        db().batch_execute(&format!(
            "
            create sequence {nsp}.failed_records;

            create function {nsp}.fail_error_record() returns trigger
            language plpgsql as $$
            begin
                perform nextval('{nsp}.failed_records');
                raise exception 'injected outage: database unavailable';
            end;
            $$;

            create trigger fail_error_record_{nsp}
                before update on subgraphs.deployment
                for each row
                when (new.id = {id} and new.fatal_error is not null)
                execute function {nsp}.fail_error_record();
            "
        ))
        .unwrap();
        ErrorRecordingOutage { nsp, ended: false }
    }

    fn failed_records(&self) -> i64 {
        let nsp = &self.nsp;
        diesel::select(sql::<BigInt>(&format!(
            "(select case when is_called then last_value else 0 end from {nsp}.failed_records)"
        )))
        .get_result(&mut db())
        .unwrap()
    }

    fn end(&mut self) {
        if !self.ended {
            let nsp = &self.nsp;
            db().batch_execute(&format!(
                "drop trigger fail_error_record_{nsp} on subgraphs.deployment;"
            ))
            .unwrap();
            self.ended = true;
        }
    }
}

impl Drop for ErrorRecordingOutage {
    fn drop(&mut self) {
        self.end();
    }
}

fn head_number(ctx: &TestContext) -> BlockNumber {
    let id = ctx.deployment.id;
    diesel::select(sql::<Integer>(&format!(
        "(select block_number from subgraphs.head where id = {id})"
    )))
    .get_result(&mut db())
    .unwrap()
}

type RunningRunner = Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>>;

/// The runner's future is not `Send`, so the test polls it instead of spawning it.
fn start_runner(
    runner: graph_core::subgraph::SubgraphRunner<
        graph_chain_ethereum::Chain,
        graph_runtime_wasm::RuntimeHostBuilder<graph_chain_ethereum::Chain>,
    >,
) -> RunningRunner {
    Box::pin(async move { runner.run_for_test(false).await.map(|_| ()) })
}

async fn outage_test_context(name: &str) -> (TestContext, BlockPtr) {
    configure_writer_for_injected_failures();

    let RunnerTestRecipe { stores, test_info } = RunnerTestRecipe::new(name, "end-block").await;

    let mut blocks = vec![genesis()];
    for number in 1..=5 {
        let mut block = empty_block(blocks.last().unwrap().ptr(), test_ptr(number));
        push_test_log(&mut block, number.to_string());
        blocks.push(block);
    }

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let mut env_vars = EnvVars::default();
    env_vars.subgraph_error_retry_ceil = Duration::from_secs(1);
    let ctx = fixture::setup(&test_info, &stores, &chain, None, Some(env_vars)).await;
    (ctx, test_ptr(5))
}

#[graph::test]
async fn store_errors_while_handling_a_non_deterministic_error_keep_the_runner() {
    let _one_test = ONE_TEST_AT_A_TIME.lock().await;
    let (ctx, stop_block) = outage_test_context("store_errors_while_handling_keep_runner").await;

    let nsp = fail_first_write_reaching(&ctx, 2);
    let mut outage = Outage::after_injected_failure(nsp, "deployment");

    let mut run = start_runner(ctx.runner(stop_block.clone()).await);
    // `health()` and `fail_subgraph()` in the non-deterministic error arm.
    wait_for_failures(|| outage.failed_reads(), 2, &mut run).await;
    outage.end();

    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the runner did not finish after the outage")
        .unwrap();

    let head = ctx
        .store
        .least_block_ptr(&ctx.deployment.hash)
        .await
        .unwrap();
    assert_eq!(head, Some(stop_block));
    let status = ctx.indexing_status().await;
    assert!(status.fatal_error.is_none());
}

#[graph::test]
async fn store_errors_at_restart_back_off_and_retry() {
    let _one_test = ONE_TEST_AT_A_TIME.lock().await;
    let (ctx, stop_block) = outage_test_context("store_errors_at_restart_retry").await;

    let nsp = fail_first_write_reaching(&ctx, 2);
    let mut outage = Outage::after_injected_failure(nsp, "head");

    let mut run = start_runner(ctx.runner(stop_block.clone()).await);
    // Two `store.restart()` calls, so the first one was retried.
    wait_for_failures(|| outage.failed_reads(), 2, &mut run).await;
    assert_eq!(outage.head_behind_outage(&ctx), 1);
    outage.end();

    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the runner did not finish after the outage")
        .unwrap();

    let head = ctx
        .store
        .least_block_ptr(&ctx.deployment.hash)
        .await
        .unwrap();
    assert_eq!(head, Some(stop_block));
    let status = ctx.indexing_status().await;
    assert!(status.fatal_error.is_none());
}

#[graph::test]
async fn unassigned_runner_stops_while_restart_backs_off() {
    let _one_test = ONE_TEST_AT_A_TIME.lock().await;
    let (ctx, stop_block) = outage_test_context("unassigned_runner_stops_in_backoff").await;

    let nsp = fail_first_write_reaching(&ctx, 2);
    let outage = Outage::after_injected_failure(nsp, "head");

    let runner = ctx.runner(stop_block).await;
    let instances = runner.context().instances.clone();
    let mut run = start_runner(runner);
    wait_for_failures(|| outage.failed_reads(), 2, &mut run).await;

    instances.remove(&ctx.deployment.id);

    // One backoff interval is at most `subgraph_error_retry_ceil` (1s) plus its jitter.
    let result = tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .expect("the unassigned runner kept retrying");
    if let Err(e) = result {
        assert!(!format!("{e:#}").contains("injected outage"), "{e:#}");
    }
}

#[graph::test]
async fn deterministic_error_is_recorded_without_passing_its_block() {
    let _one_test = ONE_TEST_AT_A_TIME.lock().await;
    configure_writer_for_injected_failures();

    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("deterministic_error_recorded_in_place", "fatal-error").await;

    let mut blocks = vec![genesis()];
    for number in 1..=5 {
        blocks.push(empty_block(blocks.last().unwrap().ptr(), test_ptr(number)));
    }

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let mut env_vars = EnvVars::default();
    env_vars.subgraph_error_retry_ceil = Duration::from_secs(1);
    let ctx = fixture::setup(&test_info, &stores, &chain, None, Some(env_vars)).await;

    let mut outage = ErrorRecordingOutage::start(&ctx);

    let mut run = start_runner(ctx.runner(test_ptr(5)).await);
    // The fatal-error subgraph fails deterministically at block 3.
    wait_for_failures(|| outage.failed_records(), 2, &mut run).await;
    assert!(head_number(&ctx) <= 3);
    outage.end();

    let result = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the runner did not finish after the outage");
    assert!(result.is_err());

    assert!(head_number(&ctx) <= 3);
    let status = ctx.indexing_status().await;
    let err = status.fatal_error.unwrap();
    assert_eq!(err.block.number, 3.into());
    assert!(err.deterministic);
}
