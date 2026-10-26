#  Copyright 2023 EPAM Systems
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import threading
import time
from queue import Empty, PriorityQueue, Queue
from typing import Any, Callable, Optional, Protocol

from opensearchpy import ConflictError, NotFoundError
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from app.amqp.amqp import AmqpClient
from app.commons import logging
from app.commons.model.launch_objects import ApplicationConfig, SearchConfig
from app.commons.model.processing import ProcessingItem, ProcessingResult
from app.commons.processing import DummyProcessor, Processor, RealProcessor, Worker
from app.service.processor import ServiceProcessor

QUEUE_POSSIBLE_BUG = "Received result but no tasks in running queue, possible bug"

LOGGER = logging.getLogger("analyzerApp.amqpHandler")
NUMBER_OF_TASKS_TO_SEND = 2


def log_incoming_message(routing_key: str, correlation_id: str, body: Any) -> None:
    body_str = json.dumps(body)
    LOGGER.debug(
        f"Processing message: --Routing key: {routing_key} --Correlation ID: {correlation_id} --Body: {body_str}"
    )


def log_outgoing_message(reply_to: str, correlation_id: str, body: Any) -> None:
    body_str = json.dumps(body)
    LOGGER.debug(f"Replying message: --To: {reply_to} --Correlation ID: {correlation_id} --Body: {body_str}")


def serialize_message(channel: BlockingChannel, delivery_tag: Optional[int], body: bytes) -> Optional[Any]:
    """Parse the message body.

    Rejects a message that is not valid JSON and acks an empty one, since callers drop both. Callers settle
    the other messages once they are processed. Must run on the connection thread.
    """
    try:
        message = json.loads(body, strict=False)
    except Exception as exc:
        LOGGER.exception("Failed to parse message body to JSON", exc_info=exc)
        if delivery_tag is not None:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
        return None
    if not message and delivery_tag is not None:
        channel.basic_ack(delivery_tag=delivery_tag)
    return message


_SETTLE_LOCK = threading.Lock()


def settle_message(item: ProcessingItem, requeue: bool = False) -> None:
    """Ack the AMQP message of a processing item, or nack it with requeue.

    Safe to call from any thread: pika's ``BlockingChannel`` is not thread-safe, so the ack runs on the
    connection thread through ``add_callback_threadsafe``. The item is settled once, later calls do nothing.
    If the channel is closed by then, the broker has already requeued the message and nothing is sent.

    :param ProcessingItem item: The processing item to settle
    :param bool requeue: Nack the message with requeue instead of acking it
    """
    with _SETTLE_LOCK:
        channel = item.channel
        item.channel = None
    delivery_tag = item.delivery_tag
    if channel is None or delivery_tag is None:
        return

    def _settle() -> None:
        if not channel.is_open:
            LOGGER.warning(f"AMQP channel closed, the broker requeued message {delivery_tag}")
            return
        try:
            if requeue:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            else:
                channel.basic_ack(delivery_tag=delivery_tag)
        except Exception as exc:
            LOGGER.exception(f"Failed to settle message {delivery_tag}", exc_info=exc)

    try:
        channel.connection.add_callback_threadsafe(_settle)
    except Exception as exc:
        LOGGER.warning(f"AMQP connection closed, the broker requeued message {delivery_tag}", exc_info=exc)


def reply_to_dropped(
    channel: BlockingChannel,
    routing_key: str,
    props: BasicProperties,
    dropped_reply: Optional[Callable[[str], Optional[str]]],
) -> None:
    """Reply to an RPC request the routing key predicate dropped, so the caller does not wait for a timeout.

    Must run on the connection thread.
    """
    if not dropped_reply or not props.reply_to:
        return
    body = dropped_reply(routing_key)
    if body is None:
        return
    try:
        channel.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=BasicProperties(correlation_id=props.correlation_id, content_type="application/json"),
            mandatory=False,
            body=bytes(body, "utf-8"),
        )
        LOGGER.debug(
            f"Replied to dropped request: --Routing key: {routing_key} --Correlation ID: {props.correlation_id}"
        )
    except Exception as exc:
        LOGGER.exception("Failed to reply to dropped request", exc_info=exc)


def get_priority(props: BasicProperties) -> int:
    """Determine priority from timestamp_in_ms header or use current time."""
    priority = int(time.time() * 1000)  # Default to current time in milliseconds
    if props.headers and "timestamp_in_ms" in props.headers:
        try:
            timestamp_str = str(props.headers["timestamp_in_ms"])
            # Remove 'L' suffix if present (e.g., 1749029201296L)
            if timestamp_str.endswith("L"):
                timestamp_str = timestamp_str[:-1]
            priority = int(timestamp_str)
        except (ValueError, TypeError) as exc:
            LOGGER.warning(f"Failed to parse timestamp_in_ms header: {props.headers['timestamp_in_ms']}", exc_info=exc)
    return priority


def retry(item: ProcessingItem, exc: Optional[Exception]) -> bool:
    """Default retry predicate for handling exceptions."""
    # You can customize this logic based on your requirements
    if exc is None:
        return True
    if isinstance(exc, ConflictError) and item.routing_key == "item_remove":
        return "but no document was found" not in exc.error
    if isinstance(exc, NotFoundError) and item.routing_key == "remove_by_launch_start_time":
        return "no such index" not in exc.error
    if isinstance(exc, ValueError) and item.routing_key == "train_models":
        return "Input X contains NaN" not in str(exc)
    return True


class AtomicInteger:
    """Thread-safe integer counter with atomic increment and decrement operations.

    Provides thread-safe operations for incrementing, decrementing, and reading integer values
    using a threading lock to ensure atomicity across multiple threads.
    """

    def __init__(self, value: int = 0) -> None:
        """Initialize the atomic integer with an optional starting value.

        :param int value: Initial value for the counter (default: 0)
        """
        self._value = int(value)
        self._lock = threading.Lock()

    def inc(self, d: int = 1) -> int:
        """Atomically increment the value and return the new value.

        :param int d: Amount to increment by (default: 1)
        :return: The new value after incrementing
        :rtype: int
        """
        with self._lock:
            self._value += int(d)
            return self._value

    def dec(self, d: int = 1) -> int:
        """Atomically decrement the value and return the new value.

        :param int d: Amount to decrement by (default: 1)
        :return: The new value after decrementing
        :rtype: int
        """
        return self.inc(-d)

    @property
    def value(self) -> int:
        """Get the current value in a thread-safe manner.

        :return: Current integer value
        :rtype: int
        """
        with self._lock:
            return self._value


class AmqpRequestHandler(Protocol):
    processor: Processor
    running_tasks: list[ProcessingItem]

    def handle_amqp_request(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        props: BasicProperties,
        body: bytes,
    ) -> None:
        """Handle AMQP request."""
        ...


class ProcessAmqpRequestHandler:
    """Class for handling AMQP requests with process-based routing and priority queue."""

    app_config: ApplicationConfig
    search_config: SearchConfig
    client: AmqpClient
    queue_size: int
    routing_key_predicate: Callable[[str], bool]
    dropped_reply: Optional[Callable[[str], Optional[str]]]
    counter: AtomicInteger
    queue: PriorityQueue[ProcessingItem]
    __running_tasks: Queue[ProcessingItem]
    processor: Processor
    _monitor_thread: Optional[threading.Thread]
    _sender_thread: Optional[threading.Thread]
    _receiver_thread: Optional[threading.Thread]
    _shutdown: bool
    _init_services: set[str]
    _retry_predicate: Optional[Callable[[ProcessingItem, Exception], bool]]

    def __init__(
        self,
        app_config: ApplicationConfig,
        search_config: SearchConfig,
        *,
        queue_size: Optional[int] = None,
        routing_key_predicate: Optional[Callable[[str], bool]] = None,
        dropped_reply: Optional[Callable[[str], Optional[str]]] = None,
        client: Optional[AmqpClient] = None,
        init_services: Optional[list[str]] = None,
        retry_predicate: Optional[Callable[[ProcessingItem, Optional[Exception]], bool]] = retry,
        name: Optional[str] = None,
    ):
        """Initialize handler for processing requests with process-based communication.

        :param ApplicationConfig app_config: Application configuration object
        :param SearchConfig search_config: Search configuration object
        :param Optional[int] queue_size: Maximum size of the internal priority queue (buffer), defaults to
        ``amqpPrefetchCount`` since RabbitMQ never delivers more unacknowledged messages than that
        :param Optional[Callable[[str], bool]] routing_key_predicate: Optional predicate to filter routing keys for
        processing. Should return True for keys to process.
        :param Optional[Callable[[str], Optional[str]]] dropped_reply: Optional function that returns the reply body
        for an RPC request the routing key predicate drops, or None to send no reply
        :param Optional[AmqpClient] client: Optional AMQP client for sending replies (useful for testing)
        :param Optional[list[str]] init_services: Optional list of routing keys to initialize the processor with
        specific routing keys
        :param Optional[Callable[[ProcessingItem, Exception], bool]] retry_predicate: Optional function to determine
        if a failed processing attempt should be retried
        :param Optional[str] name: Optional name for the processing thread
        """
        self.app_config = app_config
        self.search_config = search_config
        self.client = client or AmqpClient(app_config)
        self.queue_size = queue_size if queue_size is not None else app_config.amqpPrefetchCount
        self.routing_key_predicate = routing_key_predicate or (lambda _: True)
        self.dropped_reply = dropped_reply
        if init_services:
            self._init_services = set(init_services)
        else:
            self._init_services = set()
        self._retry_predicate = retry_predicate
        self.counter = AtomicInteger(0)

        # Initialize queue and running tasks
        self.queue = PriorityQueue(maxsize=self.queue_size)
        self.__running_tasks = Queue()

        # Setup process communication
        self.processor = RealProcessor(
            app_config,
            search_config,
            Worker(self._init_services).work,
        )
        self._shutdown = False

        # Start the three processing threads
        thread_name_prefix = name or "ProcessHandler"
        self._monitor_thread = threading.Thread(
            target=self._monitor_processor, name=f"{thread_name_prefix}-Monitor", daemon=True
        )
        self._sender_thread = threading.Thread(
            target=self._send_tasks, name=f"{thread_name_prefix}-Sender", daemon=True
        )
        self._receiver_thread = threading.Thread(
            target=self._receive_results, name=f"{thread_name_prefix}-Receiver", daemon=True
        )

        self._monitor_thread.start()
        self._sender_thread.start()
        self._receiver_thread.start()

    def __send_task_lock(self, processing_item: ProcessingItem) -> None:
        """Send processing item to processor process"""
        self.processor.send(processing_item)
        self.__running_tasks.put(processing_item)
        processing_item.send_time = time.time()  # Record send time for tracking

    def __send_to_processor(self) -> Optional[ProcessingItem]:
        processing_item: Optional[ProcessingItem] = None
        try:
            processing_item = self.queue.get_nowait()
            logging.set_correlation_id(processing_item.log_correlation_id)
            channel = processing_item.channel
            if channel is not None and not channel.is_open:
                # The broker requeued the message when the channel closed and delivers it again
                LOGGER.info(
                    f"AMQP channel closed, skipping message {processing_item.routing_key} - "
                    f"{processing_item.msg_correlation_id}"
                )
                return processing_item
            log_incoming_message(processing_item.routing_key, processing_item.msg_correlation_id, processing_item.item)
            self.__send_task_lock(processing_item)
            return processing_item
        except Empty:
            return None
        except Exception as exc:
            LOGGER.exception("Failed to send message to processor", exc_info=exc)
            if processing_item is not None:
                settle_message(processing_item, requeue=True)
            return None

    def __send_task_no_lock(self, processing_item: ProcessingItem) -> None:
        """Send processing item to processor process without acquiring a lock."""
        self.processor.send(processing_item)
        self.__running_tasks.queue.append(processing_item)
        processing_item.send_time = time.time()  # Record send time for tracking

    def _restart_processor(self) -> None:
        """Restart the processor process if it has died"""
        if self._shutdown:
            # If shutdown is initiated, do not restart the processor
            return

        # Clean up old process and connections
        self.processor.shutdown()

        # Create new processor instance
        self.processor = RealProcessor(
            self.app_config,
            self.search_config,
            Worker(self._init_services).work,
        )
        LOGGER.info("Successfully restarted processor process")

        if self.__running_tasks.qsize() <= 0:
            LOGGER.info("No running tasks to re-send to new processor")
            return

        # Store running tasks to re-send them, clearing the queue
        with self.__running_tasks.mutex:
            # Double-check under mutex if the queue is empty
            if not self.__running_tasks.queue:
                return

            tasks_to_resend: list[ProcessingItem] = list(self.__running_tasks.queue)
            self.__running_tasks.queue.clear()
            if not tasks_to_resend:
                return

            # Increment retries for the first task, since it might be the one that caused the failure
            tasks_to_resend[0].retries += 1

            # Re-send the first task to the new processor, it keeps its retry count this way. Return the other
            # tasks, which had not started, to RabbitMQ, so any consumer can take them.
            resent = 0
            requeued = 0
            for i, task in enumerate(tasks_to_resend):
                if task.retries >= self.app_config.amqpHandlerMaxRetries:
                    LOGGER.warning(
                        f"Task exceeded max retries, skipping. --Routing key: {task.routing_key}"
                        f" --Correlation ID: {task.msg_correlation_id} Body: {json.dumps(task.item)}",
                        correlation_id=task.log_correlation_id,
                    )
                    settle_message(task)
                    continue
                if i > 0 and task.channel is not None:
                    settle_message(task, requeue=True)
                    requeued += 1
                    continue
                try:
                    self.__send_task_no_lock(task)
                    resent += 1
                    LOGGER.debug(f"Re-sent task to new processor: {task.routing_key} - {task.msg_correlation_id}")
                except Exception as exc:
                    LOGGER.exception(f"Failed to re-send task {task.routing_key} to new processor", exc_info=exc)
                    settle_message(task, requeue=True)
            LOGGER.info(f"Re-sent {resent} tasks to new processor, returned {requeued} tasks to the queue")

    def __has_long_running_tasks(self) -> bool:
        """Check if there are any long-running tasks that need attention"""
        if not self.__running_tasks.queue:
            return False

        # Check if any task has been running longer than the configured timeout
        with self.__running_tasks.mutex:
            if not self.__running_tasks.queue:
                return False
            for task in self.__running_tasks.queue:
                if task.send_time and (time.time() - task.send_time) > self.app_config.amqpHandlerTaskTimeout:
                    LOGGER.warning(f"Task {task.routing_key} - {task.msg_correlation_id} is taking too long")
                    return True
        return False

    def __handle_response(self, result: ProcessingResult):
        response_body = result.result
        if response_body is None:
            return None
        logging.set_correlation_id(result.item.log_correlation_id)
        try:
            if result.item.reply_to:
                log_outgoing_message(result.item.reply_to, result.item.msg_correlation_id, response_body)
                self.client.reply(result.item.reply_to, result.item.msg_correlation_id, response_body)
        except Exception as exc:
            LOGGER.exception("Failed to publish result", exc_info=exc)
            return None

        LOGGER.debug("Finished processing response")
        return None

    def __remove_last_task_from_queue(self, result: ProcessingResult) -> Optional[ProcessingItem]:
        """Remove the finished task from the running tasks and return it."""
        try:
            completed_task = self.__running_tasks.get_nowait()
            if completed_task.msg_correlation_id != result.item.msg_correlation_id:
                LOGGER.warning(
                    f"Received result for task {result.item.msg_correlation_id} but expected "
                    f"{completed_task.msg_correlation_id}. Possible mismatch."
                )
            return completed_task
        except Empty:
            LOGGER.warning(QUEUE_POSSIBLE_BUG)
            return None

    def __process_result(self, result: ProcessingResult) -> None:
        # Handle successful result
        if result.success:
            # Remove the completed task from running queue
            completed_task = self.__remove_last_task_from_queue(result)
            if result.item.retries > 0:
                LOGGER.warning(
                    f"Task {result.item.routing_key} - {result.item.msg_correlation_id} "
                    f"was retried {result.item.retries} times"
                )
            self.__handle_response(result)
            # Ack after the reply is published, so a crash in between redelivers the message
            if completed_task:
                settle_message(completed_task)
            return

        # Handle failed result - check if we should retry
        should_retry = True
        if self._retry_predicate is not None:
            should_retry = self._retry_predicate(result.item, result.error)
            if not should_retry:
                LOGGER.info(
                    f"Retry predicate failed for message {result.item.routing_key} - "
                    f"{result.item.msg_correlation_id}, not retrying"
                )

        # Check if we exceeded max retries
        exceeded_max_retries = result.item.retries >= self.app_config.amqpHandlerMaxRetries

        # If we should NOT retry, remove the task from running queue
        if not should_retry or exceeded_max_retries:
            completed_task = self.__remove_last_task_from_queue(result)
            if completed_task:
                settle_message(completed_task)
            LOGGER.error(
                f"Task failed after {result.item.retries} retries. --Routing key: {result.item.routing_key}"
                f" --Correlation ID: {result.item.msg_correlation_id} Body: {json.dumps(result.item.item)}"
            )
            return

        # We should retry - update the task in place (without removing from running_tasks)
        with self.__running_tasks.mutex:
            if self.__running_tasks.queue:
                # Get original tracking task and use it (without removing it)
                task_to_retry = self.__running_tasks.queue[0]

                # Verify it matches the result
                if task_to_retry.msg_correlation_id != result.item.msg_correlation_id:
                    LOGGER.warning(
                        f"Received result for task {result.item.msg_correlation_id} but expected "
                        f"{task_to_retry.msg_correlation_id}. Possible mismatch."
                    )
            else:
                LOGGER.warning(QUEUE_POSSIBLE_BUG)
                task_to_retry = result.item

            # Update the task
            task_to_retry.retries += 1
            task_to_retry.send_time = time.time()

            # Resend to processor
            try:
                self.processor.send(task_to_retry)
                LOGGER.debug(
                    f"Retrying task {task_to_retry.routing_key} - {task_to_retry.msg_correlation_id}, "
                    f"attempt {task_to_retry.retries} of {self.app_config.amqpHandlerMaxRetries}"
                )
            except Exception as exc:
                LOGGER.exception(f"Failed to retry task {task_to_retry.routing_key}", exc_info=exc)

    def __receive_results(self) -> None:
        try:
            if not self.processor.poll(timeout=0.1):
                return
            result: ProcessingResult = self.processor.recv()
            self.__process_result(result)
        except Exception as exc:
            LOGGER.exception("Failed to receive response from processor", exc_info=exc)

    def _monitor_processor(self):
        """Thread 1: Monitor processor and task states"""
        while not self._shutdown:
            try:
                # Check if processor process is alive and restart if needed
                if not self.processor.is_alive():
                    LOGGER.warning("Processor process is not running, restarting...")
                    self._restart_processor()

                if self.__has_long_running_tasks():
                    LOGGER.warning("Detected long-running tasks, restarting processor...")
                    self._restart_processor()

                # Monitor interval
                time.sleep(0.1)

            except Exception as exc:
                LOGGER.exception("Error in monitor thread", exc_info=exc)
                time.sleep(0.1)

    def _send_tasks(self):
        """Thread 2: Send tasks to processor if running tasks < NUMBER_OF_TASKS_TO_SEND"""
        while not self._shutdown:
            try:
                # Send messages to processor if we have less than 2 running tasks
                while self.__running_tasks.qsize() < NUMBER_OF_TASKS_TO_SEND:
                    if not self.__send_to_processor():
                        break

                # Small sleep to prevent busy waiting
                if self.queue.empty():
                    time.sleep(0.01)

            except Exception as exc:
                LOGGER.exception("Error in sender thread", exc_info=exc)
                time.sleep(0.1)

    def _receive_results(self):
        """Thread 3: Receive results from processor if there are running tasks"""
        while not self._shutdown:
            try:
                if not self.__running_tasks.empty():
                    self.__receive_results()
                else:
                    # Small sleep when no running tasks
                    time.sleep(0.01)

            except Exception as exc:
                LOGGER.exception("Error in receiver thread", exc_info=exc)
                time.sleep(0.1)

    def handle_amqp_request(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        props: BasicProperties,
        body: bytes,
    ) -> None:
        """Function for handling amqp request: convert to ProcessingItem and add to priority queue.

        The message stays unacknowledged until its task finishes, so the RabbitMQ prefetch limits how many
        messages this handler holds.
        """
        number: int = self.counter.inc()
        logging.new_correlation_id()

        routing_key = method.routing_key or ""
        if not self.routing_key_predicate(routing_key):
            reply_to_dropped(channel, routing_key, props, self.dropped_reply)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return None

        # Processing request
        message = serialize_message(channel, method.delivery_tag, body)
        if not message:
            return None

        priority = get_priority(props)

        # Create ProcessingItem
        processing_item = ProcessingItem(
            priority=priority,
            number=number,
            routing_key=routing_key,
            reply_to=props.reply_to,
            log_correlation_id=logging.get_correlation_id(),
            msg_correlation_id=props.correlation_id or "",
            item=message,
            delivery_tag=method.delivery_tag,
            channel=channel,
        )

        # Add to priority queue (blocking on overflow)
        try:
            self.queue.put(processing_item, block=True)
        except Exception as exc:
            LOGGER.exception("Failed to add item to processing queue", exc_info=exc)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return None
        return None

    @property
    def running_tasks(self) -> list[ProcessingItem]:
        """Get the queue of currently running tasks."""
        if not self.__running_tasks.queue:
            return []
        with self.__running_tasks.mutex:
            if not self.__running_tasks.queue:
                return []
            return list(self.__running_tasks.queue)

    def shutdown(self):
        """Gracefully shutdown the handler"""
        self._shutdown = True

        # Stop processor process
        self.processor.shutdown()

        # Wait for all processing threads
        for thread in [self._monitor_thread, self._sender_thread, self._receiver_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5)


class DirectAmqpRequestHandler:
    """Class for direct handling AMQP requests. Use for debug purpose."""

    app_config: ApplicationConfig
    search_config: SearchConfig
    routing_key_predicate: Callable[[str], bool]
    dropped_reply: Optional[Callable[[str], Optional[str]]]
    __running_tasks: Queue[ProcessingItem]
    counter: int
    processor: Processor
    _init_services: set[str]
    _processor: ServiceProcessor

    def __init__(
        self,
        app_config: ApplicationConfig,
        search_config: SearchConfig,
        *,
        routing_key_predicate: Optional[Callable[[str], bool]] = None,
        dropped_reply: Optional[Callable[[str], Optional[str]]] = None,
        init_services: Optional[list[str]] = None,
        **_: Any,
    ):
        """Initialize handler for processing requests.

        :param app_config: Application configuration object
        :param search_config: Search configuration object
        :param routing_key_predicate: Optional predicate to filter routing keys for processing. Should return True for
        keys to process.
        :param dropped_reply: Optional function that returns the reply body for an RPC request the routing key
        predicate drops, or None to send no reply
        :param init_services: Optional list of routing keys to initialize the processor with specific routing keys
        """
        self.app_config = app_config
        self.search_config = search_config
        self.routing_key_predicate = routing_key_predicate or (lambda _: True)
        self.dropped_reply = dropped_reply
        self.__running_tasks = Queue()
        if init_services:
            self._init_services = set(init_services)
        else:
            self._init_services = set()
        self.counter = 0
        self.processor = DummyProcessor()
        self._processor = ServiceProcessor(app_config, search_config, services_to_init=self._init_services)

    def handle_amqp_request(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        props: BasicProperties,
        body: bytes,
    ) -> None:
        """Function for handling amqp request: index, search and analyze using routing key."""
        logging.new_correlation_id()

        routing_key = method.routing_key or ""
        if not self.routing_key_predicate(routing_key):
            reply_to_dropped(channel, routing_key, props, self.dropped_reply)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return None

        # Processing request
        message = serialize_message(channel, method.delivery_tag, body)
        if not message:
            return None

        try:
            self.__process_message(channel, method, props, routing_key, message)
        finally:
            # Ack once processing finishes, successful or not
            channel.basic_ack(delivery_tag=method.delivery_tag)
        return None

    def __process_message(
        self,
        channel: BlockingChannel,
        method: Basic.Deliver,
        props: BasicProperties,
        routing_key: str,
        message: Any,
    ) -> None:
        """Process a parsed message and publish the response."""
        self.counter += 1
        log_incoming_message(routing_key, props.correlation_id or "", message)

        self.__running_tasks.put(
            ProcessingItem(
                priority=get_priority(props),
                number=self.counter,
                routing_key=routing_key,
                reply_to=props.reply_to,
                log_correlation_id=logging.get_correlation_id(),
                msg_correlation_id=props.correlation_id or "",
                item=message,
            )
        )

        # Process using the processor
        try:
            response_body = self._processor.process(method.routing_key or "", message)
        except Exception as exc:
            LOGGER.exception("Failed to process message", exc_info=exc)
            self.__running_tasks.get()
            return None

        # Sending response if applicable
        if response_body is None:
            self.__running_tasks.get()
            return None

        try:
            if props.reply_to:
                channel.basic_publish(
                    exchange="",
                    routing_key=props.reply_to,
                    properties=BasicProperties(correlation_id=props.correlation_id, content_type="application/json"),
                    mandatory=False,
                    body=bytes(response_body, "utf-8"),
                )
        except Exception as exc:
            LOGGER.exception("Failed to publish result", exc_info=exc)
            self.__running_tasks.get()
            return None
        LOGGER.debug("Finished processing response")
        self.__running_tasks.get()
        return None

    @property
    def running_tasks(self) -> list[ProcessingItem]:
        """Get the queue of currently running tasks."""
        if not self.__running_tasks.queue:
            return []
        with self.__running_tasks.mutex:
            if not self.__running_tasks.queue:
                return []
            return list(self.__running_tasks.queue)
