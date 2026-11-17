use std::sync::Arc;
use std::time::Duration;

use graph::env::EnvVars;
use graph::prelude::diesel::connection::SimpleConnection;
use graph::prelude::diesel::dsl::sql;
use graph::prelude::diesel::sql_types::Text;
use graph::prelude::diesel::{self, Connection, PgConnection, RunQueryDsl};
use graph::prelude::{BlockNumber, SubgraphStore as _};
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

/// Fails the first transaction that moves the deployment head to `block` or
/// later; the sequence is not rolled back with it, so later writes succeed.
fn fail_first_write_reaching(ctx: &TestContext, block: BlockNumber) {
    let id = ctx.deployment.id;
    let nsp: String = diesel::select(sql::<Text>(&format!(
        "(select name from deployment_schemas where id = {id})"
    )))
    .get_result(&mut db())
    .unwrap();
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
