# -*- coding: utf-8 -*-
"""
API to create, update, delete and query Skyline Test Monitor results and steps.
"""
from __future__ import absolute_import

# Import python libs
import datetime
import logging

# Import local libs
# pylint: disable=import-error
from systemlink.messagebus.amqp_connection_manager import AmqpConnectionManager
from systemlink.messagebus.exceptions import SystemLinkException
from systemlink.messagebus.message_service import MessageService
from systemlink.messagebus.message_service_builder import MessageServiceBuilder
from . import messages as testmon_messages  # pylint: disable=no-name-in-module
# pylint: enable=import-error

# Set up logging
LOGGER = logging.getLogger(__name__)


class TestMonitorClient():
    """
    Class to publicly access the Test Monitor Client.
    """
    def __init__(self,  # pylint: disable=too-many-arguments
                 message_service=None,
                 service_name='TestMonitorClient',
                 config=None,
                 connection_timeout=5,
                 auto_reconnect=True):
        """
        :param message_service: An instance of the message
            service to use or ``None`` to allow this object to create and own
            the message service.
        :type message_service: systemlink.messagebus.message_service.MessageService or None
        :param service_name: If `message_service` is ``None`` and therefore
            this object creates and owns the message service, the name to
            use for this message service. Try to pick a unique name for
            your client.
        :type service_name: str or None
        :param config: If ``message_service`` is ``None``, the configuration to use for this
            message service. If this is ``None``, will use the default configuration.
        :type config: systemlink.messagebus.amqp_configuration.AmqpConfiguration or None
        :param connection_timeout: Timeout, in seconds, to use
            when trying to connect to the message broker.
        :type connection_timeout: float or int
        """
        self._closing = False
        self._own_message_service = False
        self._connection_manager = None
        if message_service:
            self._message_service = message_service
        else:
            self._connection_manager = AmqpConnectionManager(config=config)
            self._connection_manager.connection_timeout = connection_timeout
            self._connection_manager.auto_reconnect = auto_reconnect
            message_service_builder = MessageServiceBuilder(service_name)
            message_service_builder.connection_manager = self._connection_manager
            self._message_service = MessageService(message_service_builder)
            self._own_message_service = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        """
        Close the TestMonitorClient and all associated resources.
        """
        if self._closing:
            return
        self._closing = True
        if self._own_message_service:
            self._message_service.close()
            self._connection_manager.close()

    def create_results(self, results):
        """
        Create one or more test results

        :param results: A list of dicts. Each dict must have the following keys:
            status (:class:`systemlink.testmonclient.messages.Status` or `str`):
                    Test status.
            startedAt (:class:`datetime`):
                    Test start time.
            programName (:class:`str`):
                    Test program name.
            systemId (:class:`str`):
                    Identifier for the system that ran this test.
            hostName (:class:`str`):
                    Host machine name.
            operator (:class:`str`):
                    Operator name for this result.
            serialNumber (:class:`str`):
                    Serial number for the Unit Under Test.
            totalTimeInSeconds (:class:`float` or :class:`int`):
                    Test duration time in seconds.
            keywords (``list(str)``):
                    A list of keywords associated with the result.
            properties (``dict(str)``):
                    Key/value pairs of properties associated with the result.
            fileIds (``list(str)``):
                    List of fileIds associated with the result.
        :type results: list(dict)
        """
        result_create_requests = []
        for test in results:
            test_status = test['status']
            if isinstance(test_status, str):
                test_status = testmon_messages.Status(
                    testmon_messages.StatusType.from_string(test_status.upper()), test_status)
                test['status'] = test_status.to_dict()
            result_create_request = testmon_messages.ResultCreateRequest.from_dict(test)
            result_create_requests.append(result_create_request)

        request = testmon_messages.TestMonitorCreateTestResultsRequest(result_create_requests)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorCreateTestResultsResponse.from_message(generic_message)
        LOGGER.debug('message = %s', res)

        return res

    def update_results(self, updates, replace=False, determine_status_from_steps=False):
        """
        Update one or more test results

        :param updates: A list of dicts. Each dict must have the following keys:
            status (:class:`systemlink.testmonclient.messages.Status` or `str`):
                    Test status.
            startedAt (:class:`datetime`):
                    Test start time.
            programName (:class:`str`):
                    Test program name.
            systemId (:class:`str`):
                    Identifier for the system that ran this test.
            operator (:class:`str`):
                    Operator name for this result.
            serialNumber (:class:`str`):
                    Serial number for the Unit Under Test.
            totalTimeInSeconds (:class:`float` or :class:`int`):
                    Test duration time in seconds.
            keywords (``list(str)``):
                    A list of keywords associated with the result.
            properties (``dict(str)``):
                    Key/value pairs of properties associated with the result.
            fileIds (``list(str)``):
                    List of fileIds associated with the result.
        :type updates: list(dict)
        :param replace: Indicates if keywords, properties, and file ids should replace the existing
            collection, or be merged with the existing collection.
        :type replace: bool
        :param determine_status_from_steps: Indicates if the status should be set based on the
            status of the related steps.
        :type determine_status_from_steps: bool
        """
        result_update_requests = []
        for update in updates:
            test_status = update['status']
            if isinstance(test_status, str):
                test_status = testmon_messages.Status(
                    testmon_messages.StatusType.from_string(test_status.upper()),
                    test_status)
                update['status'] = test_status.to_dict()
            result_update_request = testmon_messages.ResultUpdateRequest.from_dict(update)
            result_update_requests.append(result_update_request)

        request = testmon_messages.TestMonitorUpdateTestResultsRequest(
            result_update_requests,
            replace,
            determine_status_from_steps)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorUpdateTestResultsResponse.from_message(generic_message)
        LOGGER.debug('message = %s', res)

        return res

    def delete_results(self, ids, delete_steps=True):
        """
        Delete one or more test results

        :param ids: A list of the IDs of the results to delete.
        :type ids: list(str)
        :param delete_steps: Whether or not to delete the results corresponding steps.
        :type delete_steps: bool
        """
        request = testmon_messages.TestMonitorDeleteResultsRequest(ids, delete_steps)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorDeleteResultsResponse.from_message(generic_message)
        LOGGER.debug('message = %s', res)

        return res

    def delete_all_results(self):
        """
        Delete all results
        """
        routed_message = testmon_messages.TestMonitorDeleteAllResultsRoutedMessage()
        self._message_service.publish_routed_message(routed_message)

    def query_results(self, query=None, skip=0, take=-1):
        """
        Return results that match query

        :param query: Object indicating query parameters.
        :type query: systemlink.testmonclient.messages.ResultQuery
        :param skip: Number of results to skip before searching.
        :type skip: int
        :param take: Maximum number of results to return.
        :type take: int
        :return: Results that matched the query.
        :rtype: tuple(list(systemlink.testmonclient.messages.ResultResponse), int)
        """
        request = testmon_messages.TestMonitorQueryResultsRequest(query, skip, take)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorQueryResultsResponse.from_message(generic_message)
        LOGGER.debug('TotalCount: %d', res.total_count)

        return res.results, res.total_count

    def create_steps(self, steps):
        """
        Create one or more step results

        :param steps: A list of dicts. Each dict must have the following keys:
            name (:class:`str`):
                    Step name.
            stepType (:class:`str`):
                    Step type.
            stepId (:class:`str`):
                    Step Identifier.
            parentId (:class:`str`):
                    Identifier for the step parent.
            resultId (:class:`str`):
                    Identifier for the result that this step is associated with.
            status (:class:`systemlink.testmonclient.messages.Status` or `str`):
                    Step status.
            totalTimeInSeconds (:class:`float` or :class:`int`):
                    Step duration time in seconds.
            startedAt (:class:`datetime`):
                    Step start time.
            dataModel (:class:`str`):
                    The name of the data structure in the stepData list. This value identifies
                    the key names that exist in the stepData parameters object. It is generally
                    used to provide context for custom UIs to know what values are expected.
            stepData (``list(stepData)``):
                    A list of data objects for the step.  Each element contains a text string and
                    a parameters dictionary of string:string key-value pairs.
            children (``list(str)``):
                    A list of step ids that define other steps in the request that are children of
                    this step.  The ids in this list must exist as objects in the in the request.
        """
        step_create_requests = []
        for step in steps:
            step_status = step['status']
            if isinstance(step_status, str):
                step_status = testmon_messages.Status(
                    testmon_messages.StatusType.from_string(step_status.upper()),
                    step_status)
                step['status'] = step_status.to_dict()
            step_create_request = testmon_messages.StepCreateRequest.from_dict(step)
            step_create_requests.append(step_create_request)

        request = testmon_messages.TestMonitorCreateTestStepsRequest(step_create_requests)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorCreateTestStepsResponse.from_message(generic_message)
        LOGGER.debug('message = %s', res)

        return res

    def update_steps(self, steps):
        """
        Update one or more steps

        :param updates: A list of dicts. Each dict must at least have step_id and result_id. The
            other dict members are used to update the step, if present:
            name (:class:`str`):
                    Step name.
            stepType (:class:`str`):
                    Step type
            stepId (:class:`str`):
                    Step Identifier.
            parentId (:class:`str`):
                    Identifier for the step parent.
            resultId (:class:`str`):
                    Identifier for the result that this step is associated with.
            status (:class:`systemlink.testmonclient.messages.Status` or `str`):
                    Step status.
            totalTimeInSeconds (:class:`float` or :class:`int`):
                    Step duration time in seconds.
            startedAt (:class:`datetime`):
                    Step start time.
            dataModel (:class:`str`):
                    The name of the data structure in the stepData list.  This value identifies
                    the key names that exist in the stepData parameters object.  It is generally
                    used to provide context for custom UIs to know what values are expected.
            stepData (``list(stepData)``):
                    A list of data objects for the step.  Each element contains a text string and
                    a parameters dictionary of string:string key-value pairs.
            children (``list(str)``):
                    A list of step ids that define other steps in the request that are children of
                    this step.  The ids in this list must exist as objects in the in the request.
        """
        step_update_requests = []
        for step in steps:
            if 'status' in step:
                step_status = step['status']
                if isinstance(step_status, str):
                    step_status = testmon_messages.Status(
                        testmon_messages.StatusType.from_string(step_status.upper()),
                        step_status)
                    step['status'] = step_status.to_dict()
            step_update_request = testmon_messages.StepUpdateRequest.from_dict(step)
            step_update_requests.append(step_update_request)
        request = testmon_messages.TestMonitorUpdateTestStepsRequest(step_update_requests)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorUpdateTestStepsResponse.from_message(generic_message)
        LOGGER.debug('message = %s', res)

        return res

    def delete_steps(self, steps):
        """
        Delete one or more steps

        :param steps: A list of dicts. Each dict must have the following keys:
            stepId (:class:`str`):
                    Step Identifier.
            resultId (:class:`str`):
                    Identifier for the result that this step is associated with.
        :type steps: list(dict)
        """
        delete_steps = []

        for step in steps:
            delete_step = testmon_messages.StepDeleteRequest.from_dict(step)
            delete_steps.append(delete_step)

        request = testmon_messages.TestMonitorDeleteStepsRequest(delete_steps)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorDeleteStepsResponse.from_message(generic_message)
        LOGGER.debug('message = %s', res)

        return res

    def query_steps(self, query=None, skip=0, take=-1):
        """
        Return steps that match query

        :param query: Object indicating query parameters.
        :type query: systemlink.testmonclient.messages.StepQuery
        :param skip: Number of steps to skip before searching.
        :type skip: int
        :param take: Maximum number of steps to return.
        :type take: int
        :return: Results that matched the query.
        :rtype: tuple(list(systemlink.testmonclient.messages.StepResponse), int)
        """

        request = testmon_messages.TestMonitorQueryStepsRequest(query, skip, take)
        generic_message = self._message_service.publish_synchronous_message(request)
        if generic_message is None:
            raise SystemLinkException.from_name('Skyline.RequestTimedOut')
        if generic_message.has_error():
            raise SystemLinkException(error=generic_message.error)
        LOGGER.debug('generic_message = %s', generic_message)
        res = testmon_messages.TestMonitorQueryStepsResponse.from_message(generic_message)
        LOGGER.debug('TotalCount: %d', res.total_count)

        return res.steps, res.total_count
