import asyncio
import os
import sys
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            say_hello, name, start_to_close_timeout=timedelta(seconds=10)
        )


async def run_worker(client: Client, task_queue: str) -> None:
    print(f"[worker] connected, polling task_queue={task_queue}", flush=True)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[HelloWorkflow],
        activities=[say_hello],
    ):
        await asyncio.Future()


async def run_starter(client: Client, task_queue: str) -> None:
    wf_id = f"hello-{os.environ.get('HOSTNAME', 'cron')}"
    print(f"[starter] executing workflow id={wf_id}", flush=True)
    result = await client.execute_workflow(
        HelloWorkflow.run,
        "world",
        id=wf_id,
        task_queue=task_queue,
    )
    print(f"[starter] result={result!r}", flush=True)
    if result != "Hello, world!":
        raise SystemExit(f"unexpected result: {result!r}")


async def main() -> None:
    address = os.environ.get("TEMPORAL_URL") or os.environ["TEMPORAL_ADDRESS"]
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "hello-task-queue")
    mode = sys.argv[1] if len(sys.argv) > 1 else "worker"
    print(f"[{mode}] connecting to {address} ns={namespace}", flush=True)
    client = await Client.connect(address, namespace=namespace)
    if mode == "worker":
        await run_worker(client, task_queue)
    elif mode == "starter":
        await run_starter(client, task_queue)
    else:
        raise SystemExit(f"unknown mode={mode}")


if __name__ == "__main__":
    asyncio.run(main())
