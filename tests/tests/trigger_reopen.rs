use std::time::Duration;

use graph::blockchain::BlockPtr;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::components::store::DeploymentLocator;
use graph::prelude::diesel::connection::SimpleConnection;
use graph::prelude::diesel::dsl::sql;
use graph::prelude::diesel::sql_types::{Array, Text};
use graph::prelude::diesel::{self, Connection, PgConnection, RunQueryDsl};
use graph::prelude::{BlockNumber, SubgraphStore as _};
use graph_chain_ethereum::Chain;
use graph_tests::fixture::ethereum::{chain, empty_block, genesis, push_test_log};
use graph_tests::fixture::{self, test_ptr, test_ptr_reorged};
use graph_tests::recipe::RunnerTestRecipe;
use tokio::sync::Mutex;

const RETIRED: &str = "A";
const CONTROL: &str = "C";
const RETIRE_AT: BlockNumber = 3;

static ONE_DEPLOYMENT_AT_A_TIME: Mutex<()> = Mutex::const_new(());

struct Versions {
    retired: Vec<String>,
    control: Vec<String>,
}

fn block(
    parent: &BlockWithTriggers<Chain>,
    ptr: BlockPtr,
    ids: &[&str],
) -> BlockWithTriggers<Chain> {
    let mut block = empty_block(parent.ptr(), ptr);
    for id in ids {
        push_test_log(&mut block, *id);
    }
    block
}

fn blocks_up_to_reopen() -> Vec<BlockWithTriggers<Chain>> {
    let block_0 = genesis();
    let block_1 = block(&block_0, test_ptr(1), &[RETIRED]);
    let block_2 = block(&block_1, test_ptr(2), &[RETIRED]);
    let block_3 = block(&block_2, test_ptr(3), &[]);
    let block_4 = block(&block_3, test_ptr(4), &[CONTROL]);
    let block_5 = block(&block_4, test_ptr(5), &[RETIRED, CONTROL]);
    vec![block_0, block_1, block_2, block_3, block_4, block_5]
}

fn db() -> PgConnection {
    let url = std::env::var("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("THEGRAPH_STORE_POSTGRES_DIESEL_URL must be set");
    PgConnection::establish(&url).expect("connect to the test database")
}

fn namespace(deployment: &DeploymentLocator) -> String {
    let query = format!(
        "(select name from deployment_schemas where id = {})",
        deployment.id
    );
    diesel::select(sql::<Text>(&query))
        .get_result(&mut db())
        .unwrap()
}

/// Retires every open trigger the way an external process does it, inside
/// the transaction that writes block `RETIRE_AT`, while the runner keeps
/// its entity cache.
fn retire_open_triggers_when_block_is_written(deployment: &DeploymentLocator) {
    let nsp = namespace(deployment);
    let id = deployment.id;
    db().batch_execute(&format!(
        "
        create function {nsp}.retire_open_triggers() returns trigger
        language plpgsql as $$
        begin
            update {nsp}.position_update_trigger
               set block_range = int4range(lower(block_range), lower(block_range) + 1)
             where upper_inf(block_range);
            return null;
        end;
        $$;

        create trigger retire_open_triggers_{nsp}
            after update of block_number on subgraphs.head
            for each row
            when (new.id = {id} and new.block_number = {RETIRE_AT})
            execute function {nsp}.retire_open_triggers();
        "
    ))
    .unwrap();
}

fn block_ranges(deployment: &DeploymentLocator, id: &str) -> Vec<String> {
    let nsp = namespace(deployment);
    let query = format!(
        "array(select block_range::text from {nsp}.position_update_trigger \
         where id = '{id}' order by lower(block_range))"
    );
    diesel::select(sql::<Array<Text>>(&query))
        .get_result(&mut db())
        .unwrap()
}

async fn index(test_name: &str, blocks: Vec<BlockWithTriggers<Chain>>) -> Versions {
    let _one_deployment = ONE_DEPLOYMENT_AT_A_TIME.lock().await;
    unsafe {
        std::env::set_var("GRAPH_STORE_WRITE_QUEUE", "0");
        std::env::set_var("GRAPH_ENTITY_CACHE_EXCLUDE", "PositionUpdateTrigger");
    }

    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new(test_name, "trigger-reopen").await;
    let stop_block = blocks.last().unwrap().ptr();
    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    retire_open_triggers_when_block_is_written(&ctx.deployment);
    ctx.start_and_sync_to(stop_block).await;

    let versions = Versions {
        retired: block_ranges(&ctx.deployment, RETIRED),
        control: block_ranges(&ctx.deployment, CONTROL),
    };

    let store = ctx.store.clone();
    let hash = ctx.deployment.hash.clone();
    drop(ctx);
    for _ in 0..100 {
        if store.locators(&hash).await.unwrap().is_empty() {
            return versions;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("deployment {hash} was not removed after the test");
}

#[graph::test]
async fn trigger_reopen() {
    let mut blocks = blocks_up_to_reopen();
    let block_6 = block(blocks.last().unwrap(), test_ptr(6), &[RETIRED, CONTROL]);
    let block_7 = block(&block_6, test_ptr(7), &[RETIRED, CONTROL]);
    blocks.extend([block_6, block_7]);

    let versions = index("trigger_reopen", blocks).await;

    assert_eq!(versions.retired, ["[1,2)", "[5,)"]);
    assert_eq!(versions.control, ["[4,)"]);
}

#[graph::test]
async fn trigger_reopen_reorg() {
    let mut blocks = blocks_up_to_reopen();
    let block_4 = &blocks[4];
    let block_5_reorged = block(block_4, test_ptr_reorged(5, 1), &[CONTROL]);
    let block_6_reorged = block(
        &block_5_reorged,
        test_ptr_reorged(6, 1),
        &[RETIRED, CONTROL],
    );
    blocks.extend([block_5_reorged, block_6_reorged]);

    let versions = index("trigger_reopen_reorg", blocks).await;

    assert_eq!(versions.retired, ["[1,2)", "[6,)"]);
    assert_eq!(versions.control, ["[4,)"]);
}
