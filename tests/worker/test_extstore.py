import dataclasses
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Self

import pytest

import temporalio
import temporalio.converter
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowFailureError
from temporalio.converter import (
    ActivitySerializationContext,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.exceptions import ActivityError, TimeoutError
from temporalio.extstore import (
    ExternalStorageClaim,
    ExternalStorageDriver,
    ExternalStorageDriverContext,
    ExternalStorageDriverError,
    ExternalStorageOptions,
)
from temporalio.testing._workflow import WorkflowEnvironment
from tests.helpers import new_worker


@dataclass(frozen=True)
class ExtStoreActivityInput:
    input_data: str
    output_size: int
    pass


@activity.defn
async def ext_store_activity(
    input: ExtStoreActivityInput,
) -> str:
    return "ao" * int(input.output_size / 2)


@dataclass(frozen=True)
class ExtStoreWorkflowInput:
    input_data: str
    activity_input_size: int
    activity_output_size: int
    output_size: int
    pass


@workflow.defn
class ExtStoreWorkflow:
    @workflow.run
    async def run(self, input: ExtStoreWorkflowInput) -> str:
        await workflow.execute_activity(
            ext_store_activity,
            ExtStoreActivityInput(
                input_data="ai" * int(input.activity_input_size / 2),
                output_size=input.activity_output_size,
            ),
            schedule_to_close_timeout=timedelta(seconds=3),
        )
        return "wo" * int(input.output_size / 2)


class TestBaseDriver(ExternalStorageDriver, WithSerializationContext):
    def __init__(self, driver_name: str, driver_type: str):
        self._driver_name = driver_name
        self._driver_type = driver_type
        self._storage: dict[str, bytes] = {}
        self.context: SerializationContext

    def name(self) -> str:
        return self._driver_name

    def type(self) -> str:
        return self._driver_type

    async def store(
        self,
        context: ExternalStorageDriverContext,
        payloads: Sequence[Payload],
    ) -> list[ExternalStorageClaim | None]:
        start_index = len(self._storage)

        entries = [
            (f"payload-{start_index + i}", payload.SerializeToString())
            for i, payload in enumerate(payloads)
        ]
        self._storage.update(entries)

        return [ExternalStorageClaim(data={"key": key}) for key, _ in entries]

    async def retrieve(
        self,
        context: ExternalStorageDriverContext,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[Payload]:
        def parse_claim(
            claim: ExternalStorageClaim,
        ) -> Payload:
            key = claim.data["key"]
            if key not in self._storage:
                raise KeyError(f"Payload not found: {key}")
            payload = Payload()
            payload.ParseFromString(self._storage[key])
            return payload

        return [parse_claim(claim) for claim in claims]

    def with_context(self, context: SerializationContext) -> Self:
        self.context = context
        return self


class TestRefCountingDriver(TestBaseDriver):
    def __init__(self, driver_name: str = "temporalio:driver:testrefcounting"):
        super().__init__(driver_name, "temporalio:driver:testrefcounting")
        self.store_calls = 0
        self.retrieve_calls = 0
        self.workflow_retrieve_calls = 0
        self.workflow_store_calls = 0
        self.activity_retrieve_calls = 0
        self.activity_store_calls = 0

    async def store(
        self,
        context: ExternalStorageDriverContext,
        payloads: Sequence[Payload],
    ) -> list[ExternalStorageClaim | None]:
        self.store_calls += 1
        if isinstance(self.context, ActivitySerializationContext):
            self.activity_store_calls += 1
        if isinstance(self.context, WorkflowSerializationContext):
            self.workflow_store_calls += 1

        return await super().store(context, payloads)

    async def retrieve(
        self,
        context: ExternalStorageDriverContext,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[Payload]:
        self.retrieve_calls += 1
        print(f"Context: {self.context}")
        if isinstance(self.context, ActivitySerializationContext):
            self.activity_retrieve_calls += 1
        if isinstance(self.context, WorkflowSerializationContext):
            self.workflow_retrieve_calls += 1

        return await super().retrieve(context, claims)


async def test_extstore_small_values(env: WorkflowEnvironment):
    """All values passed through should be smaller that the default
    threshold, so the driver should not be invoked at all."""
    driver = TestRefCountingDriver()

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        await client.execute_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=10,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

    assert 0 == driver.store_calls
    assert 0 == driver.retrieve_calls


async def test_extstore_workflow_input_size(env: WorkflowEnvironment):
    """Using a small threshold, validate that workflow input size over
    the threshold causes driver to be invoked."""
    driver = TestRefCountingDriver()

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        await client.execute_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="wi" * 1024,
                activity_input_size=10,
                activity_output_size=10,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

    assert 1 == driver.store_calls
    assert 1 == driver.retrieve_calls
    assert 0 == driver.activity_store_calls
    assert 0 == driver.activity_retrieve_calls
    assert 1 == driver.workflow_store_calls
    assert 1 == driver.workflow_retrieve_calls


async def test_extstore_workflow_result_size(env: WorkflowEnvironment):
    """Using a small threshold, validate that activity result size over
    the threshold causes driver to be invoked."""
    driver = TestRefCountingDriver()

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        await client.execute_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=10,
                output_size=1000,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

    assert 1 == driver.store_calls
    assert 1 == driver.retrieve_calls
    assert 0 == driver.activity_store_calls
    assert 0 == driver.activity_retrieve_calls
    assert 1 == driver.workflow_store_calls
    assert 1 == driver.workflow_retrieve_calls


async def test_extstore_activity_input_size(env: WorkflowEnvironment):
    """Using a small threshold, validate that activity input size over
    the threshold causes driver to be invoked."""
    driver = TestRefCountingDriver()

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        await client.execute_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=1000,
                activity_output_size=10,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

    assert 1 == driver.store_calls
    assert 1 == driver.retrieve_calls
    assert 0 == driver.activity_store_calls
    assert 1 == driver.activity_retrieve_calls
    assert 1 == driver.workflow_store_calls
    assert 0 == driver.workflow_retrieve_calls


async def test_extstore_activity_result_size(env: WorkflowEnvironment):
    """Using a small threshold, validate that activity result size over
    the threshold causes driver to be invoked."""
    driver = TestRefCountingDriver()

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        await client.execute_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=1000,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

    assert 1 == driver.store_calls
    assert 1 == driver.retrieve_calls
    assert 1 == driver.activity_store_calls
    assert 0 == driver.activity_retrieve_calls
    assert 0 == driver.workflow_store_calls
    assert 1 == driver.workflow_retrieve_calls


class TestBadDriverError(Exception):
    pass


class TestBadDriver(TestBaseDriver):
    def __init__(
        self,
        driver_name: str = "temporalio:driver:testbaddriver",
        no_store: bool = False,
        no_retrieve: bool = False,
        raise_store: bool = False,
        raise_retrieve: bool = False,
    ):
        super().__init__(driver_name, "temporalio:driver:testbaddriver")
        self._no_store = no_store
        self._no_retrieve = no_retrieve
        self._raise_store = raise_store
        self._raise_retrieve = raise_retrieve

    async def store(
        self,
        context: ExternalStorageDriverContext,
        payloads: Sequence[Payload],
    ) -> list[ExternalStorageClaim | None]:
        if self._no_store:
            return []
        if self._raise_store:
            raise TestBadDriverError("raise from store")
        return await super().store(context, payloads)

    async def retrieve(
        self,
        context: ExternalStorageDriverContext,
        claims: Sequence[ExternalStorageClaim],
    ) -> list[Payload]:
        if self._no_retrieve:
            return []
        if self._raise_retrieve:
            raise TestBadDriverError("raise from retrieve")
        return await super().retrieve(context, claims)


async def test_extstore_activity_input_no_store(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that activity result size over
    the threshold causes driver to be invoked."""
    driver = TestBadDriver(no_store=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=1000,
                activity_output_size=10,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()

        assert isinstance(err.value.cause, TimeoutError)


async def test_extstore_activity_input_no_retrieve(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that activity result size over
    the threshold causes driver to be invoked."""
    driver = TestBadDriver(no_retrieve=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=1000,
                activity_output_size=10,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()

        assert isinstance(err.value.cause, ActivityError)


async def test_extstore_activity_result_no_store(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that activity result size over
    the threshold causes driver to be invoked."""
    driver = TestBadDriver(no_store=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=1000,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()

        assert isinstance(err.value.cause, ActivityError)


async def test_extstore_activity_result_no_retrieve(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that activity result size over
    the threshold causes driver to be invoked.
    """
    driver = TestBadDriver(no_retrieve=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=1000,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()

        assert isinstance(err.value.cause, TimeoutError)


async def test_extstore_workflow_input_no_store(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that when a driver incorrectly
    reports a different number of claims than provided payloads, external
    storage will raise an ExternalStorageDriverError. This happens within
    the context of a far client when trying to store workflow args.
    """
    driver = TestBadDriver(no_store=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        with pytest.raises(ExternalStorageDriverError):
            await client.execute_workflow(
                ExtStoreWorkflow.run,
                ExtStoreWorkflowInput(
                    input_data="wi" * 500,
                    activity_input_size=10,
                    activity_output_size=10,
                    output_size=10,
                ),
                id=f"workflow-{uuid.uuid4()}",
                task_queue=worker.task_queue,
                execution_timeout=timedelta(seconds=3),
            )


async def test_extstore_workflow_input_no_retrieve(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that when a driver incorrectly
    reports a different number of payloads than provided claims, external
    storage will raise an ExternalStorageDriverError. This happens within
    the context of a workflow task when trying to retrieve workflow args.
    """
    driver = TestBadDriver(no_retrieve=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="wi" * 500,
                activity_input_size=10,
                activity_output_size=10,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()

        assert isinstance(err.value.cause, TimeoutError)


async def test_extstore_workflow_result_no_store(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that when a driver incorrectly
    reports a different number of claims than provided payloads, external
    storage will raise an ExternalStorageDriverError. This happens within
    the context of a workflow task when trying to store a workflow result.
    """
    driver = TestBadDriver(no_store=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=10,
                output_size=1000,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()

        assert isinstance(err.value.cause, TimeoutError)


async def test_extstore_workflow_result_no_retrieve(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that when a driver incorrectly
    retrieves a different number of payloads for the provided claims,
    external storage will raise a ExternalStorageDriverError. This happens
    in the far client context when attempt to retrieve the workflow result.
    """
    driver = TestBadDriver(no_retrieve=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=10,
                output_size=1000,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(ExternalStorageDriverError) as err:
            await handle.result()


async def test_extstore_workflow_input_far_client_no_worker_extstore(
    env: WorkflowEnvironment,
):
    """Validate that when a worker is provided a workflow history with
    external storage references and the worker is not configured for external
    storage, it will cause a workflow task failure.
    """
    driver = TestRefCountingDriver()

    far_client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    worker_client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
    )

    async with new_worker(
        worker_client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await far_client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="wi" * 1024,
                activity_input_size=10,
                activity_output_size=10,
                output_size=10,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(WorkflowFailureError) as err:
            await handle.result()

        assert isinstance(err.value.cause, TimeoutError)


async def test_extstore_workflow_input_raise_store(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that when a driver raises an exception
    on externally storing payloads, it is wrapped in a ExternalStorageDriverError.
    """
    driver = TestBadDriver(raise_store=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        with pytest.raises(ExternalStorageDriverError) as err:
            await client.execute_workflow(
                ExtStoreWorkflow.run,
                ExtStoreWorkflowInput(
                    input_data="wi" * 500,
                    activity_input_size=10,
                    activity_output_size=10,
                    output_size=10,
                ),
                id=f"workflow-{uuid.uuid4()}",
                task_queue=worker.task_queue,
                execution_timeout=timedelta(seconds=3),
            )

        assert isinstance(err.value.__cause__, TestBadDriverError)


async def test_extstore_workflow_result_raise_retrieve(
    env: WorkflowEnvironment,
):
    """Using a small threshold, validate that when a driver raises an exception
    on retrieving external payloads, it is wrapped in a ExternalStorageDriverError.
    """
    driver = TestBadDriver(raise_retrieve=True)

    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=dataclasses.replace(
            temporalio.converter.default(),
            external_storage=ExternalStorageOptions(
                driver=driver,
                payload_size_threshold=1024,
            ),
        ),
    )

    async with new_worker(
        client, ExtStoreWorkflow, activities=[ext_store_activity]
    ) as worker:
        handle = await client.start_workflow(
            ExtStoreWorkflow.run,
            ExtStoreWorkflowInput(
                input_data="workflow input",
                activity_input_size=10,
                activity_output_size=10,
                output_size=1000,
            ),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=3),
        )

        with pytest.raises(ExternalStorageDriverError) as err:
            await handle.result()

        assert isinstance(err.value.__cause__, TestBadDriverError)
