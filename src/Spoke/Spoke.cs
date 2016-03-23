/*
    Spoke v0.09.1
    
    Spoke is a webhooks library designed to be implemented in your current service application.
    
    https://github.com/AmbitEnergyLabs/Spoke
    
    The MIT License (MIT)
    
    Copyright (c) 2016 Ambit Energy. All rights reserved.
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Jint;
using Newtonsoft.Json;
using SequelocityDotNet;
using Environment = System.Environment;

namespace Spoke
{
    /// <summary>
    /// Spoke is an http based event brokering solution.
    /// </summary>
    public class Spoke
    {
        /// <summary>
        /// The configuration object used throughout spoke.
        /// </summary>
        public static SpokeConfiguration Configuration;

        /// <summary>
        /// Initializes an instance of spoke.
        /// </summary>
        /// <param name="configuration">An instance of the <see cref="SpokeConfiguration"/></param>
        public Spoke( SpokeConfiguration configuration )
        {
            Configuration = configuration;
        }

        /// <summary>
        /// Starts the internal clock thread.
        /// </summary>
        public void Start()
        {
            InternalApi.StartClock();
        }

        /// <summary>
        /// Api methods that will be commonly used by external users.
        /// </summary>
        public class ExternalApi
        {
            /// <summary>
            /// Publishes an event
            /// </summary>
            /// <param name="systemName">Name of the calling system.</param>
            /// <param name="eventName">Name of the event you are trying to publish.</param>
            /// <param name="eventPayload">Event Payload.</param>
            /// <param name="topics">The topics you would like to publish for your event. These are the values that you can filter on when subscribing to events.</param>
            /// <returns><see cref="Models.Event"/></returns>
            public static Models.Event PublishEvent(
               string systemName,
               string eventName,
               object eventPayload,
               IDictionary<string, string> topics )
            {
                if ( topics == null )
                    topics = new Dictionary<string, string>();

                if ( !topics.ContainsKey( "SystemName" ) && !topics.ContainsKey( "SYSTEM_NAME" ) )
                {
                    topics.Add( "SystemName", systemName );
                }

                if ( !topics.ContainsKey( "EventName" ) && !topics.ContainsKey( "EVENT_NAME" ) )
                {
                    topics.Add( "EventName", eventName );
                }

                var validTopicKeys = topics.ValidateTopicKeys();

                if ( !validTopicKeys )
                {
                    throw new Exception( "Topic keys can only contain alphanumeric and underscore characters." );
                }

                topics = topics.NormalizeKeys();

                var validTopicValues = topics.ValidateTopicValues();

                if ( !validTopicValues )
                {
                    throw new Exception( "Topic values cannot be null." );
                }

                var @event = new Models.Event
                {
                    EventPayload = eventPayload,
                    Topics = topics,
                    TopicCount = topics.Count
                }.Stamp();

                @event = Configuration.Database().SaveEvent( @event, false );

                Task.Run( () =>
                {
                    InternalApi.SaveEventTopics( @event );
                } )
                .ContinueWith( x => InternalApi.ProcessEvent( @event.EventId, null ) );

                return @event;
            }

            /// <summary>
            /// Adds a new subscription.
            /// </summary>
            /// <param name="subscriptionName">The name of the subscription you would like to add.</param>
            /// <param name="serviceEndPoint">The url to your API Endpoint.</param>
            /// <param name="serviceTypeCode">The type of api. The supported values can be found by calling the GetValidServiceTypeCodes method.</param>
            /// <param name="httpMethod">The type of http method. POST or GET.</param>
            /// <param name="transformFunction">The javascript transform function that will be evaluated before a request is made to your api.</param>
            /// <param name="abortAfterMinutes">The number of minutes you would like the broker to retry failed calls to your api.</param>
            /// <param name="topics">The topics you are subscribing to.</param>
            /// <param name="requestType">The type of request to be sent to your API. "OBJECT" which will POST json to your service, "PARAMETERS" which will POST a namevaluecollection to your API, or "QUERY_STRING" which will GET from your API via query string. </param>
            /// <returns><see cref="Models.SubscriptionResponse"/></returns>
            public static Models.SubscriptionResponse AddSubscription(
                string subscriptionName,
                string serviceEndPoint,
                string serviceTypeCode,
                string httpMethod,
                string transformFunction,
                int? abortAfterMinutes,
                List<Models.SubscriptionTopic> topics,
                string requestType
                )
            {
                var subscription = InternalApi.SaveSubscription(
                    null,
                    subscriptionName,
                    Models.SubscriptionStatusCodes.Active,
                    serviceEndPoint,
                    string.IsNullOrWhiteSpace( serviceTypeCode ) 
                        ? "DEFAULT" 
                        : serviceTypeCode,
                    httpMethod,
                    transformFunction,
                    abortAfterMinutes,
                    topics.Select(
                        t => new Models.SubscriptionTopic
                        {
                            Key = t.Key,
                            Value = t.Value,
                            OperatorTypeCode = t.OperatorTypeCode
                        } ).ToList(),
                    requestType );

                return new Models.SubscriptionResponse
                {
                    Subscription = subscription
                };
            }

            /// <summary>
            /// Update an existing subscription.
            /// </summary>
            /// <param name="subscriptionId">The id of the subscription you are trying to change.</param>
            /// <param name="subscriptionName">The name of the subscription you would like to add.</param>
            /// <param name="serviceEndPoint">The url to your API Endpoint.</param>
            /// <param name="serviceTypeCode">The type of api. The supported values can be found by calling the GetValidServiceTypeCodes method.</param>
            /// <param name="httpMethod">The type of http method. POST or GET.</param>
            /// <param name="transformFunction">The javascript transform function that will be evaluated before a request is made to your api.</param>
            /// <param name="abortAfterMinutes">The number of minutes you would like the broker to retry failed calls to your api.</param>
            /// <param name="topics">The topics you are subscribing to.</param>
            /// <param name="requestType">The type of request to be sent to your API. OBJECT which will POST json to your service, PARAMETERS which will POST a namevaluecollection to your API, or QUERY_STRING which will GET from your API via query string. </param>
            /// <returns><see cref="Models.SubscriptionResponse"/></returns>
            public static Models.SubscriptionResponse UpdateSubscription(
                string subscriptionId,
                string subscriptionName,
                string serviceEndPoint,
                string serviceTypeCode,
                string httpMethod,
                string transformFunction,
                int? abortAfterMinutes,
                List<Models.SubscriptionTopic> topics,
                string requestType
                )
            {
                var subscription = InternalApi.SaveSubscription(
                    subscriptionId,
                    subscriptionName,
                    Models.SubscriptionStatusCodes.Active,
                    serviceEndPoint,
                    string.IsNullOrWhiteSpace( serviceTypeCode ) 
                        ? "DEFAULT" 
                        : serviceTypeCode,
                    httpMethod,
                    transformFunction,
                    abortAfterMinutes,
                    topics.Select(
                        t => new Models.SubscriptionTopic
                        {
                            Key = t.Key,
                            Value = t.Value,
                            OperatorTypeCode = t.OperatorTypeCode
                        } ).ToList(),
                    requestType );

                return new Models.SubscriptionResponse
                {
                    Subscription = subscription
                };
            }

            /// <summary>
            /// Change the status ( ACTIVE, INACTIVE ) of a current subscription
            /// </summary>
            /// <param name="subscriptionId">The id of the subscription you are trying to change.</param>
            /// <param name="active">Whether the subscription should be active or not. True or False</param>
            /// <returns><see cref="Models.SubscriptionResponse"/></returns>
            public static Models.SubscriptionResponse SetSubscriptionActive(
                string subscriptionId,
                bool active
                )
            {
                var subscription = InternalApi.SaveSubscriptionStatus(
                    subscriptionId,
                    active ? Models.SubscriptionStatusCodes.Active : Models.SubscriptionStatusCodes.Inactive );

                return new Models.SubscriptionResponse
                {
                    Subscription = subscription
                };
            }
            
            /// <summary>
            /// Soft delete a current subscription. This will update the status of the subscription to deleted which is similar to setting one to inactive.
            /// </summary>
            /// <param name="subscriptionId">The id of the subscription you are trying to remove.</param>
            /// <returns><see cref="Models.SubscriptionResponse"/></returns>
            public static Models.SubscriptionResponse DeleteSubscription(
                string subscriptionId
                )
            {
                var subscription = InternalApi.SaveSubscriptionStatus(
                    subscriptionId,
                    Models.SubscriptionStatusCodes.Deleted
                     );

                return new Models.SubscriptionResponse
                {
                    Subscription = subscription
                };
            }

            /// <summary>
            /// Get subscription by subscription id or subscription name.
            /// </summary>
            /// <param name="subscriptionId">The id of the subscription you are trying to retrieve</param>
            /// <param name="subscriptionName">The name of the subscription you are trying to retrieve</param>
            /// <returns><see cref="Models.GetSubscriptionResponse"/></returns>
            public static Models.GetSubscriptionResponse GetSubscription(
                string subscriptionId,
                string subscriptionName
                )
            {
                var subscription = Configuration.Database().GetSubscription( subscriptionId, subscriptionName );

                return new Models.GetSubscriptionResponse
                {
                    Subscription = subscription
                };
            }

            /// <summary>
            /// Get a distinct list of names of all events that have been published.
            /// </summary>
            /// <returns><see cref="Models.EventNamesResponse"/></returns>
            public static Models.EventNamesResponse GetAllEventNames()
            {
                var eventNames = Configuration.Database().GetAllEventNames();

                return new Models.EventNamesResponse
                {
                    EventNames = eventNames
                };
            }

            /// <summary>
            /// Get a distinct list of topics.
            /// </summary>
            /// <returns><see cref="Models.TopicKeysResponse"/></returns>
            public static Models.TopicKeysResponse GetAllTopicKeys()
            {
                var topicKeys = Configuration.Database().GetAllTopicKeys();

                return new Models.TopicKeysResponse
                {
                    TopicKeys = topicKeys
                };
            }

            /// <summary>
            /// Get a list of supported operators. Ex. IN, NOT IN, LIKE, EQUALS
            /// </summary>
            /// <returns>List of <see cref="string"/></returns>
            public static List<string> GetValidOperatorTypeCodes()
            {
                return new List<string> { Utils.Operator.Equal, Utils.Operator.Like, Utils.Operator.In, Utils.Operator.NotIn };
            }

            /// <summary>
            /// Get a list of valid service type codes. By default the only one will be "DEFAULT".
            /// </summary>
            /// <returns><see cref="Models.ServiceTypeCodesResponse"/></returns>
            public static Models.ServiceTypeCodesResponse GetValidServiceTypeCodes()
            {
                return new Models.ServiceTypeCodesResponse
                {
                    ValidServiceTypeCodes = Configuration.WasApiCallSuccessfulHandlers.Keys.ToList()
                };
            }
        }

        /// <summary>
        /// Api methods that will be commonly used by internal users. These methods should generally not be exposed to external users.
        /// </summary>
        public class InternalApi
        {
            #region Public Methods
            /// <summary>
            /// Identify and publish and missing clock messages within the given time frame.
            /// </summary>
            /// <param name="totalMinutes">The number of minutes you want to look back.</param>
            /// <param name="offsetMinutes">The number of buffer minutes you want to have between now and the timeframe you are looking at.</param>
            /// <returns>List of <see cref="Models.ClockEvent"/></returns>
            public static List<Models.ClockEvent> BackfillClockMessages( 
                int? totalMinutes,
                int? offsetMinutes )
            {
                var mutex = Configuration.Database().TryAcquireMutex(
                    "clock-event-backfill",
                    TimeSpan.FromMinutes( Configuration.ClockBackfillMutexTimeToLiveMinutes.ToInt() )
                    );

                if ( mutex == null )
                {
                    return new List<Models.ClockEvent>();
                }

                var missingClockEvents = Configuration.Database().GetMissingClockEvents( totalMinutes, offsetMinutes )
                    .OrderBy( m => m.Year )
                    .ThenBy( m => m.Month )
                    .ThenBy( m => m.Day )
                    .ThenBy( m => m.Hour )
                    .ThenBy( m => m.Minute );

                foreach ( var minute in missingClockEvents )
                {
                    var dt = new DateTime(
                        minute.Year,
                        minute.Month,
                        minute.Day,
                        minute.Hour,
                        minute.Minute,
                        0 );

                    PublishClockEvent( dt );
                }

                Configuration.Database().ReleaseMutex( mutex );

                return missingClockEvents.ToList();
            }

            /// <summary>
            /// Generate a notification for a subscription on an event.
            /// </summary>
            /// <param name="event">The event you are generating a notification for.</param>
            /// <param name="subscription">The subscription you are sending the notification to.</param>
            /// <returns><see cref="Models.SubscriptionNotification"/></returns>
            public static Models.SubscriptionNotification GenerateSubscriptionNotification(
                Models.Event @event,
                Models.Subscription subscription )
            {
                return GenerateSubscriptionNotifications(
                    @event,
                    new List<Models.Subscription> { subscription } ).FirstOrDefault();
            }

            /// <summary>
            /// Get an event by the eventid
            /// </summary>
            /// <param name="eventId"></param>
            /// <returns><see cref="Models.EventResponse"/></returns>
            public static Models.EventResponse GetEvent(
                            string eventId
                            )
            {
                var @event = Configuration.Database().GetEvent( eventId );

                return new Models.EventResponse
                {
                    Event = @event
                };
            }

            /// <summary>
            /// Get events by count, eventName, and/or topicKey
            /// </summary>
            /// <param name="count">The number of events you want to retrieve</param>
            /// <param name="eventName">The name of the events you are trying to retrieve</param>
            /// <param name="topicKey">The name of the topic you are looking for</param>
            /// <returns><see cref="Models.EventsResponse"/></returns>
            public static Models.EventsResponse GetEvents(
                int? count,
                string eventName,
                string topicKey
                )
            {
                var events = Configuration.Database().GetEvents(
                    count,
                    eventName,
                    topicKey)
                    .ToList();

                return new Models.EventsResponse
                {
                    Events = new List<Models.Event>(events)
                };
            }

            /// <summary>
            /// Get events by count, eventName, and/or topicKey
            /// </summary>
            /// <param name="count">The maximum number of events to search through</param>
            /// <param name="eventName">The name of the events you are trying to retrieve</param>
            /// <param name="topicKey">The name of the topic you are looking for</param>
            /// <returns><see cref="Models.EventsResponse"/></returns>
            public static Models.EventsResponse GetLatestEvents(
                int? count,
                string eventName,
                string topicKey
                )
            {
                var events = Configuration.Database().GetLatestEvents(
                    count,
                    eventName,
                    topicKey )
                    .ToList();

                return new Models.EventsResponse
                {
                    Events = new List<Models.Event>( events )
                };
            }

            /// <summary>
            /// Get all subscription related to an event by event id.
            /// </summary>
            /// <param name="eventId">The id of the event you are looking for the subscriptions to.</param>
            /// <returns><see cref="Models.GetEventSubscriptionsResponse"/></returns>
            public static Models.GetEventSubscriptionsResponse GetEventSubscriptions(
                string eventId
                )
            {
                var eventSubscriptions = Configuration.Database().GetEventSubscriptions( eventId, true );

                return new Models.GetEventSubscriptionsResponse
                {
                    EventSubscriptions = new List<dynamic>( eventSubscriptions )
                };
            }

            /// <summary>
            /// Get any failed events within the given time frame.
            /// </summary>
            /// <param name="lookbackMinutes">The number of minutes you want to look back.</param>
            /// <param name="lookbackUpToMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
            /// <returns><see cref="Models.EventsResponse"/></returns>
            public static Models.EventsResponse GetFailedEvents(
               int? lookbackMinutes,
               int? lookbackUpToMinutes
               )
            {
                var configuration = Configuration;

                if ( lookbackMinutes.HasValue ) configuration.FailedEventsLookbackMinutes = lookbackMinutes;
                if ( lookbackUpToMinutes.HasValue ) configuration.FailedEventsThresholdMinutes = lookbackUpToMinutes;

                var events = Configuration.Database().GetFailedEvents();

                return new Models.EventsResponse
                {
                    Events = new List<Models.Event>( events )
                };
            }

            /// <summary>
            /// Get any failed notifications within the give time frame
            /// </summary>
            /// <param name="lookbackMinutes">The number of minutes you want to look back.</param>
            /// <param name="lookbackUpToMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
            /// <returns>List of <see cref="Models.SubscriptionNotification"/></returns>
            public static List<Models.SubscriptionNotification> GetFailedSubscriptionNotifications(
                int? lookbackMinutes = null,
                int? lookbackUpToMinutes = null
                )
            {
                var eventSubscriptions = Configuration.Database().GetFailedEventSubscriptions( lookbackMinutes, lookbackUpToMinutes );

                var subscriptionNotifications = new ConcurrentBag<Models.SubscriptionNotification>();

                Parallel.ForEach( eventSubscriptions.GroupBy( x => x.EventId ).Select( x => x.First().Event ),
                    new ParallelOptions { MaxDegreeOfParallelism = 8 },
                    @event =>
                    {
                        var subscriptions = eventSubscriptions.Where( x => x.EventId == @event.EventId ).Select( x => x.Subscription );

                        var notifications = GenerateSubscriptionNotifications( @event, subscriptions );

                        notifications.ForEach( subscriptionNotifications.Add );
                    } );

                return subscriptionNotifications.ToList();
            }

            /// <summary>
            /// Get the latest activity for an event or a subscription.
            /// </summary>
            /// <param name="eventId">The id of the event you want activity for.</param>
            /// <param name="subscriptionId">The id of the subscription you want activity for.</param>
            /// <param name="count">The number of records you want to return.</param>
            /// <returns><see cref="Models.ActivityResponse"/></returns>
            public static Models.ActivityResponse GetLatestActivity(
                string eventId,
                string subscriptionId,
                int? count
                )
            {
                var activity = Configuration.Database().GetEventSubscriptionActivities( eventId, subscriptionId, null, count ?? 100 );

                return new Models.ActivityResponse
                {
                    Activity = new List<dynamic>( activity )
                };
            }

            /// <summary>
            /// Generate all notifications associated with an event.
            /// </summary>
            /// <param name="eventId">The id of the event you are generating notifications for.</param>
            /// <returns>List of <see cref="Models.SubscriptionNotification"/></returns>
            public static List<Models.SubscriptionNotification> GenerateSubscriptionNotificationsForEvent(
                 object eventId )
            {
                var @event = Configuration.Database().GetEvent( eventId );

                return GenerateSubscriptionNotificationsForEvent( @event );
            }

            /// <summary>
            /// Get a list of all subscription or active subscriptions.
            /// </summary>
            /// <param name="activeOnly">Return only active subscriptions? true or false</param>
            /// <returns><see cref="Models.GetSubscriptionsResponse"/></returns>
            public static Models.GetSubscriptionsResponse GetSubscriptions(
                bool activeOnly = true
                )
            {
                var subscriptions = Configuration.Database().GetSubscriptions( activeOnly );

                return new Models.GetSubscriptionsResponse
                {
                    Subscriptions = new List<Models.Subscription>( subscriptions )
                };
            }

            /// <summary>
            /// Process an event.
            /// </summary>
            /// <param name="eventId">The id of the event you are trying to process.</param>
            /// <param name="subscription">The subscription you want to process the event for. This is an optional parameter, which if not supplied will process for all valid subscriptions.</param>
            /// <returns><see cref="Models.ExceptionWrapperResult{dynamic}"/></returns>
            public static Models.ExceptionWrapperResult<dynamic> ProcessEvent(
                object eventId,
                Models.Subscription subscription )
            {
                var mutexKey = "process-event-" + eventId;

                var @event = Configuration.Database().GetEvent( eventId );

                if ( @event.TopicCount != @event.EventTopics.Count )
                {
                    @event.EventTopics.AddRange( SaveEventTopics( @event ) );
                }

                var result = ExceptionWrapper<object>( () =>
                {
                    var mutex = Configuration.Database().TryAcquireMutex( mutexKey,
                        TimeSpan.FromMinutes( Configuration.EventProcessingMutexTimeToLiveMinutes.ToInt() ) );

                    if ( mutex == null )
                    {
                        LogEventSubscriptionActivity(
                            eventId,
                            null,
                            Utils.EventSubscriptionActivityTypeCode.EventProcessingMutexCouldNotBeAcquired,
                            new
                            {
                                eventId,
                                subscription
                            } );

                        return new
                        {
                            Event = @event,
                            Message = "Event not process: " + Utils.EventSubscriptionActivityTypeCode.EventProcessingMutexCouldNotBeAcquired
                        };
                    }

                    var notifications = new List<Models.SubscriptionNotification>();
                    string activityTypeCode;

                    if ( subscription == null )
                    {
                        // generate new notifications and fill them in with any pre-existing eventSubscriptions
                        notifications = GenerateSubscriptionNotificationsForEvent( eventId )
                            .SetEventSubscriptionIds( Configuration.Database().GetEventSubscriptions( eventId, false ) );

                        // save new EventSubscriptions for any notifications that don't currently have one.
                        var eventSubscriptions =
                            Configuration.Database().SaveEventSubscriptions(
                                notifications.Where( x => x.EventSubscription.EventSubscriptionId == null )
                                    .Select( x => x.EventSubscription ).ToList() );

                        notifications = notifications.SetEventSubscriptionIds( eventSubscriptions );

                        activityTypeCode = Utils.EventSubscriptionActivityTypeCode.SubscriptionsFound;
                    }
                    else
                    {
                        notifications.Add( GenerateSubscriptionNotification(
                            eventId,
                            subscription ) );

                        activityTypeCode = Utils.EventSubscriptionActivityTypeCode.InvokeServiceRequestGenerated;
                    }

                    LogEventSubscriptionActivity(
                        eventId,
                        null,
                        activityTypeCode,
                        new
                        {
                            notifications
                        } );

                    ProcessNotifications( notifications );

                    Configuration.Database().ReleaseMutex( mutex );

                    return new
                    {
                        Event = @event,
                        SubscriptionNotifications = notifications
                    };
                },
                    eventId,
                    null,
                    mutexKey,
                    true );

                return result;
            }

            /// <summary>
            /// Process a list of notifications
            /// </summary>
            /// <param name="notifications">List of <see cref="Models.SubscriptionNotification"/></param>
            public static void ProcessNotifications(
               IEnumerable<Models.SubscriptionNotification> notifications )
            {
                foreach ( var notification in notifications )
                {
                    var lNotification = notification;

                    ExecuteAsyncNotification( lNotification,
                        response => HandleHttpResponse(
                            response,
                            lNotification ) );
                }
            }

            /// <summary>
            /// Publish a clock event.
            /// </summary>
            /// <param name="time">The time that you want to publish an event for.</param>
            /// <returns><see cref="Models.Event"/></returns>
            public static Models.Event PublishClockEvent( DateTime time )
            {
                return ExternalApi.PublishEvent(
                    "ClockEventGenerator",
                    "ClockEvent",
                    null,
                    new Dictionary<string, string>
                    {
                    {"Year", time.Year.ToString(CultureInfo.InvariantCulture)},
                    {"Month", time.Month.ToString(CultureInfo.InvariantCulture)},
                    {"Day", time.Day.ToString(CultureInfo.InvariantCulture)},
                    {"DayOfWeek", time.DayOfWeek.ToString().ToUpper()},
                    {"Hour", time.Hour.ToString(CultureInfo.InvariantCulture)},
                    {"Minute", time.Minute.ToString(CultureInfo.InvariantCulture)},
                    } );
            }

            /// <summary>
            /// Re-publish an event.
            /// </summary>
            /// <param name="eventId">The id of the event you would like to republish.</param>
            /// <returns><see cref="Models.PublishEventResponse"/></returns>
            public static Models.PublishEventResponse ResendEvent(
                string eventId
                )
            {
                var @event = Configuration.Database().GetEvent( eventId );

                @event = ExternalApi.PublishEvent( @event.Topics[ "SYSTEM_NAME" ], @event.Topics[ "EVENT_NAME" ], @event.EventPayload, @event.Topics );

                return new Models.PublishEventResponse
                {
                    EventId = @event.EventId.ToString(),
                    Event = @event
                };
            }

            /// <summary>
            /// Save event topics
            /// </summary>
            /// <param name="event">The event you are saving topics for.</param>
            /// <returns>List of <see cref="Models.EventTopic"/></returns>
            public static List<Models.EventTopic> SaveEventTopics(
                Models.Event @event )
            {
                var eventTopics = @event.Topics.Select(
                    kvp => new Models.EventTopic
                    {
                        EventId = @event.EventId,
                        Key = kvp.Key,
                        Value = kvp.Value
                    }.Stamp() )
                    .Where( x => @event.EventTopics.All( t => t.Key != x.Key ) )
                    .ToList();

                return Configuration.Database().SaveEventTopics( eventTopics );
            }

            /// <summary>
            /// Save the status of a subscription
            /// </summary>
            /// <param name="subscriptionId">The id of the subscription you are trying to save.</param>
            /// <param name="subscriptionStatusCode">The status you are trying to save.</param>
            /// <returns></returns>
            public static Models.Subscription SaveSubscriptionStatus(
               object subscriptionId,
               string subscriptionStatusCode )
            {
                var subscription = Configuration.Database().GetSubscription( subscriptionId, null );

                if ( subscription == null )
                    throw new Exception( "Subscription not found" );

                return SaveSubscription(
                    subscriptionId,
                    subscription.SubscriptionName,
                    subscriptionStatusCode,
                    subscription.ApiEndpoint,
                    subscription.ApiType,
                    subscription.HttpMethod,
                    subscription.TransformFunction,
                    subscription.AbortAfterMinutes,
                    subscription.Topics,
                    subscription.RequestType );
            }

            /// <summary>
            /// Save a subscription.
            /// </summary>
            /// <param name="subscriptionId">The id of the subscription you are trying to save.</param>
            /// <param name="subscriptionName">The name of the subscription you would like to add.</param>
            /// <param name="subscriptionStatusCode">The status of the subscription you are trying to save.</param>
            /// <param name="apiEndpoint">The url to your API Endpoint.</param>
            /// <param name="apiType">The type of api. The supported values can be found by calling the GetValidServiceTypeCodes method.</param>
            /// <param name="httpMethod">The type of http method. POST or GET.</param>
            /// <param name="transformFunction">The javascript transform function that will be evaluated before a request is made to your api.</param>
            /// <param name="abortAfterMinutes">The number of minutes you would like the broker to retry failed calls to your api.</param>
            /// <param name="topics">The topics you are subscribing to.</param>
            /// <param name="requestType">The type of request to be sent to your API. OBJECT which will POST json to your service, PARAMETERS which will POST a namevaluecollection to your API, or QUERY_STRING which will GET from your API via query string. </param>
            /// <returns><see cref="Models.SubscriptionResponse"/></returns>
            public static Models.Subscription SaveSubscription(
                object subscriptionId,
                string subscriptionName,
                string subscriptionStatusCode,
                string apiEndpoint,
                string apiType,
                string httpMethod,
                string transformFunction,
                int? abortAfterMinutes,
                IEnumerable<Models.SubscriptionTopic> topics,
                string requestType )
            {
                if ( topics == null || !topics.Any() )
                {
                    throw new Exception( "You must subscription to at least 1 topic!" );
                }

                var subscription = new Models.Subscription
                {
                    SubscriptionId = subscriptionId,
                    SubscriptionName = subscriptionName,
                    SubscriptionStatusCode = subscriptionStatusCode,
                    ApiEndpoint = apiEndpoint,
                    ApiType = apiType,
                    HttpMethod = httpMethod,
                    TransformFunction = transformFunction,
                    AbortAfterMinutes = abortAfterMinutes,
                    Topics = topics.NormalizeKeys().Select( x => x.Stamp() ).ToList(),
                    RequestType = requestType
                }.Stamp();

                return Configuration.Database().SaveSubscription( subscription );
            }

            /// <summary>
            /// Start the background clock thread.
            /// </summary>
            public static void StartClock()
            {
                Task.Run( () =>
                {
                    while ( true )
                    {
                        try
                        {

                            var mutex = Configuration.SendClockMessages.GetValueOrDefault()
                                ? Configuration.Database().TryAcquireMutex( "clock-loop",
                                    TimeSpan.FromMinutes( 1 ).Add( TimeSpan.FromSeconds( 30 ) ) )
                                : null;

                            if ( mutex == null )
                            {
                                Thread.Sleep( Configuration.ClockSleepMilliseconds ?? 0 );
                                continue;
                            }

                            var currentMinute = DateTime.Now.Minute;

                            while ( DateTime.Now.Minute == currentMinute )
                            {
                                Thread.Sleep( 1000 );
                            }

                            PublishClockEvent( DateTime.Now );

                            // Wait until at least 30 seconds have elapsed since publishing the event.
                            // If we release the mutex too early, then the discrepancy between each
                            // server's clocks will cause duplicate clock events.
                            var waitTime = DateTime.Now.AddSeconds( 30 );
                            while ( DateTime.Now < waitTime )
                            {
                                Thread.Sleep( 1000 );
                            }

                            Configuration.Database().ReleaseMutex( mutex );
                        }
                        // ReSharper disable once EmptyGeneralCatchClause
                        catch
                        {
                            // Just eat the exception. The purpose of this try/catch block
                            // is to ensure the thread never goes down.
                        }
                    }
                    // ReSharper disable once FunctionNeverReturns
                    // We intentioanlly leave this thread running forever to continue to generate clock events.
                } );
            }

            /// <summary>
            /// Sweep any unprocessed events.
            /// </summary>
            /// <param name="lookbackMinutes">The number of minutes you want to look back.</param>
            /// <param name="lookbackUpToMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
            /// <returns><see cref="Models.TaskResponse"/></returns>
            public static Models.TaskResponse SweepUnprocessedEvents(
                int? lookbackMinutes,
                int? lookbackUpToMinutes
                )
            {
                Task.Run( () =>
                {
                    var events = GetFailedEvents( lookbackMinutes, lookbackUpToMinutes ).Events;

                    foreach ( var @event in events )
                    {
                        ProcessEvent( @event.EventId, null );
                    }
                } );

                return new Models.TaskResponse
                {
                    TaskConfiguration = new
                    {
                        FailedEventsLookbackMinutes = lookbackMinutes ?? Configuration.FailedEventsLookbackMinutes,
                        FailedEventsThresholdMinutes = lookbackUpToMinutes ?? Configuration.FailedEventsThresholdMinutes
                    },
                    Message = "Task Started."
                };
            }

            /// <summary>
            /// Sweep any failed notifications.
            /// </summary>
            /// <param name="lookbackMinutes">The number of minutes you want to look back.</param>
            /// <param name="lookbackUpToMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
            /// <returns><see cref="Models.TaskResponse"/></returns>
            public static Models.TaskResponse SweepFailedNotifications(
                int? lookbackMinutes,
                int? lookbackUpToMinutes
                )
            {
                Task.Run( () =>
                {
                    var notifications = GetFailedSubscriptionNotifications(
                        lookbackMinutes ?? Configuration.LiveRetryAbortAfterMinutes
                        , lookbackUpToMinutes ?? Configuration.FailedNotificationsThresholdMinutes
                        );

                    ProcessNotifications( notifications );
                } );

                return new Models.TaskResponse
                {
                    TaskConfiguration = new
                    {
                        LookBackMinutes = lookbackMinutes ?? Configuration.LiveRetryAbortAfterMinutes,
                        LookBackUpToMinutes = lookbackUpToMinutes ?? Configuration.FailedNotificationsThresholdMinutes
                    },
                    Message = "Task Started."
                };
            }
            #endregion

            #region Private Methods

            /// <summary>
            /// Wrapper to handle exceptions during a function call.
            /// </summary>
            /// <typeparam name="T"></typeparam>
            /// <param name="method">The function being called.</param>
            /// <param name="eventId">The id of the event.</param>
            /// <param name="eventSubscriptionId">The id of the event subscription.</param>
            /// <param name="mutexKey">The mutex key to acquire a mutex.</param>
            /// <param name="retry">Whether or not to retry.</param>
            /// <returns><see cref="Models.ExceptionWrapperResult{T}"/></returns>
            private static Models.ExceptionWrapperResult<T> ExceptionWrapper<T>(
               Func<T> method,
               object eventId,
               object eventSubscriptionId,
               string mutexKey,
               bool retry )
            {
                var timeoutMinutes = Configuration.DefaultAbortAfterMinutes.ToInt();

                var startTime = DateTime.Now;
                var timeout = TimeSpan.FromMinutes( timeoutMinutes );
                var currentWaitTime = TimeSpan.FromSeconds( 1 ).Milliseconds;

                var exceptions = new List<Exception>();

                while ( startTime + timeout > DateTime.Now )
                {
                    try
                    {
                        return new Models.ExceptionWrapperResult<T>
                        {
                            Result = method()
                        };
                    }
                    catch ( Exception ex )
                    {
                        LogEventSubscriptionActivity(
                            eventId,
                            eventSubscriptionId,
                            Utils.EventSubscriptionActivityTypeCode.EventProcessingError,
                            ex );

                        if ( !string.IsNullOrEmpty( mutexKey ) )
                        {
                            var mutex = Configuration.Database().GetActiveMutex( mutexKey );

                            Configuration.Database().ReleaseMutex( mutex );
                        }

                        exceptions.Add( ex );

                        if ( !retry )
                            break;

                        Thread.Sleep( currentWaitTime );
                        currentWaitTime = currentWaitTime + currentWaitTime;
                    }
                }

                return new Models.ExceptionWrapperResult<T>
                {
                    Exception = new AggregateException( exceptions )
                };
            }

            /// <summary>
            /// Async excute a notification
            /// </summary>
            /// <param name="notification">The notication you are trying to execute.</param>
            /// <param name="onComplete">Action to be run on completion of the notification.</param>
            /// <param name="timeout">The timeout for the operation</param>
            private static void ExecuteAsyncNotification(
                    Models.SubscriptionNotification notification,
                    Action<dynamic> onComplete,
                    TimeSpan? timeout = null )
            {
                Task.Run( () =>
                {
                    var mutexKey =
                        $"event-{notification.EventSubscription.EventId}-subscription-{notification.EventSubscription.SubscriptionId}";

                    ExceptionWrapper<object>( () =>
                    {
                        var mutex = Configuration.Database().TryAcquireMutex( mutexKey,
                            TimeSpan.FromMinutes(
                                Configuration.EventSubscriptionMutexTimeToLiveMinutes.ToInt() ) );

                        if ( mutex == null )
                        {
                            LogEventSubscriptionActivity(
                                notification.EventSubscription.Event.EventId,
                                notification.EventSubscription.EventSubscriptionId,
                                Utils.EventSubscriptionActivityTypeCode.SubscriptionRequestErrorMutexCouldNotBeAcquired,
                                new
                                {
                                    Notification = notification
                                } );

                            return null;
                        }

                        var fulfilledActivities = Configuration.Database().GetEventSubscriptionActivities(
                            notification.EventSubscription.Event.EventId,
                            notification.EventSubscription.Subscription.SubscriptionId,
                            Utils.EventSubscriptionActivityTypeCode.SubscriptionResponseOk,
                            null );

                        if ( fulfilledActivities.Any() )
                        {
                            LogEventSubscriptionActivity(
                                notification.EventSubscription.Event.EventId,
                                notification.EventSubscription.EventSubscriptionId,
                                Utils.EventSubscriptionActivityTypeCode.EventSubscriptionPreviouslyFulfilled,
                                new
                                {
                                    PreviouslyFulfilled = fulfilledActivities,
                                    Notification = notification
                                } );

                            return null;
                        }

                        if ( timeout != null )
                        {
                            Thread.Sleep( timeout.Value );
                        }

                        Task.Run( () =>
                        {
                            Models.HttpResponse response;

                            switch ( notification.EventSubscription.Subscription.RequestType )
                            {
                                case "PARAMETERS":
                                    response = HttpPostParameters( notification.Uri, Configuration.JsonSerializer.Deserialize<Dictionary<string, string>>( (string)notification.Payload ).ToNameValueCollection() );
                                    break;
                                case "QUERY_STRING":
                                    response = HttpGet( notification.Uri, Configuration.JsonSerializer.Deserialize<Dictionary<string, string>>( (string)notification.Payload ).ToNameValueCollection() );
                                    break;
                                case null:
                                case "OBJECT":
                                    response = HttpPostObject( notification.Uri, notification.Payload.ToString() );
                                    break;
                                default:
                                    throw new Exception( "Request type does not exist!" );
                            }

                            Configuration.Database().ReleaseMutex( mutex );

                            onComplete( response );
                        } );

                        LogEventSubscriptionActivity(
                            notification.EventSubscription.Event.EventId,
                            notification.EventSubscription.EventSubscriptionId,
                            Utils.EventSubscriptionActivityTypeCode.SubscriptionRequestSent,
                            notification );

                        // placeholder until this gets refactored
                        return null;
                    },
                        notification.EventSubscription.Event.EventId,
                        notification.EventSubscription.EventSubscriptionId,
                        mutexKey,
                        true );
                } );
            }

            /// <summary>
            /// Get the subscriptions matching to the event topics.
            /// </summary>
            /// <param name="eventTopics">List of topics to matching the subscription too.</param>
            /// <param name="subscriptions">List of <see cref="Models.Subscription"/></param>
            /// <returns>IEnumberable of <see cref="Models.Subscription"/></returns>
            private static IEnumerable<Models.Subscription> GetMatchingSubscriptions( IDictionary<string, string> eventTopics,
                IEnumerable<Models.Subscription> subscriptions )
            {
                var matchingSubscriptions = new ConcurrentBag<Models.Subscription>();

                Parallel.ForEach( subscriptions, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                    subscription =>
                    {
                        if ( IsSubscriptionForEvent( eventTopics, subscription.Topics ) )
                            matchingSubscriptions.Add( subscription );
                    } );

                return matchingSubscriptions.ToList();
            }

            /// <summary>
            /// Generation subscription notifications for an <see cref="Models.Event"/> and list of <see cref="Models.Subscription"/>
            /// </summary>
            /// <param name="event"></param>
            /// <param name="subscriptions"></param>
            /// <returns>List of <see cref="Models.SubscriptionNotification"/></returns>
            private static List<Models.SubscriptionNotification> GenerateSubscriptionNotifications(
                Models.Event @event,
                IEnumerable<Models.Subscription> subscriptions )
            {
                var notifications = new List<Models.SubscriptionNotification>();

                foreach ( var subscription in subscriptions )
                {
                    var uri = Configuration.GetApiUri != null
                        ? Configuration.GetApiUri(
                            new Models.GetApiUriInput
                            {
                                Uri = subscription.ApiEndpoint,
                                ApiType = subscription.ApiType
                            } )
                        : subscription.ApiEndpoint;

                    try
                    {
                        var transformOutput = ProcessTransform(
                            subscription.TransformFunction,
                            @event.EventPayload,
                            @event.Topics,
                            subscription.RequestType );

                        var liveRetryAbortAfterMinutes = subscription.AbortAfterMinutes ??
                                                         Configuration.DefaultAbortAfterMinutes ?? 0;

                        if ( liveRetryAbortAfterMinutes > (Configuration.LiveRetryAbortAfterMinutes ?? 0) )
                        {
                            liveRetryAbortAfterMinutes = Configuration.LiveRetryAbortAfterMinutes ?? 0;
                        }

                        notifications.Add(
                            new Models.SubscriptionNotification
                            {
                                EventSubscription = new Models.EventSubscription
                                {
                                    EventId = @event.EventId,
                                    SubscriptionId = subscription.SubscriptionId,
                                    Event = @event,
                                    Subscription = subscription
                                }.Stamp(),
                                Uri = uri,
                                Payload = transformOutput,
                                LiveRetryExpirationTime =
                                    @event.CreateDate.AddMinutes( liveRetryAbortAfterMinutes.ToInt() )
                            } );
                    }
                    catch ( Exception ex )
                    {
                        LogEventSubscriptionActivity(
                            @event.EventId,
                            null,
                            Utils.EventSubscriptionActivityTypeCode.SubscriptionTransformFunctionInvalid,
                            ex );

                        throw;
                    }
                }

                return notifications;
            }

            /// <summary>
            /// Generate notification for an event id and a single <see cref="Models.Subscription"/>
            /// </summary>
            /// <param name="eventId">The id of the event you are generating a notification for.</param>
            /// <param name="subscription">The subscription you are generating this event notification for.</param>
            /// <returns><see cref="Models.SubscriptionNotification"/></returns>
            private static Models.SubscriptionNotification GenerateSubscriptionNotification(
                object eventId,
                 Models.Subscription subscription )
            {
                var @event = Configuration.Database().GetEvent( eventId );

                return GenerateSubscriptionNotification(
                    @event,
                    subscription );
            }

            /// <summary>
            /// Generation notifications for all subscription related to an <see cref="Models.Event"/>
            /// </summary>
            /// <param name="event">The <see cref="Models.Event"/> you want to generate notifications for.</param>
            /// <returns>List of <see cref="Models.SubscriptionNotification"/></returns>
            private static List<Models.SubscriptionNotification> GenerateSubscriptionNotificationsForEvent(
                Models.Event @event )
            {
                var subscriptions = Configuration.Database().GetSubscriptions( true );

                return GenerateSubscriptionNotifications(
                    @event,
                    GetMatchingSubscriptions( @event.Topics, subscriptions ) );
            }

            /// <summary>
            /// Handle the http response for a notification.
            /// </summary>
            /// <param name="response">The http response</param>
            /// <param name="notification">The notication that was processed</param>
            /// <param name="backoffTime">The amount of time to way for the next retry. If this is not provided it will default to 1 second.</param>
            private static void HandleHttpResponse(
                Models.HttpResponse response,
                Models.SubscriptionNotification notification,
                TimeSpan? backoffTime = null )
            {
                var successful = ResponseIsSuccessful( response, notification.EventSubscription.Subscription.ApiType );

                LogEventSubscriptionActivity(
                    notification.EventSubscription.Event.EventId,
                    notification.EventSubscription.EventSubscriptionId,
                    successful
                        ? Utils.EventSubscriptionActivityTypeCode.SubscriptionResponseOk
                        : Utils.EventSubscriptionActivityTypeCode.SubscriptionResponseError,
                    new
                    {
                        response,
                        notification
                    } );

                if ( !successful && DateTime.Now < notification.LiveRetryExpirationTime )
                {
                    backoffTime = backoffTime?.Add( backoffTime.Value ) ?? TimeSpan.FromSeconds( 1 );

                    ExecuteAsyncNotification( notification,
                        r => HandleHttpResponse(
                            r,
                            notification,
                            backoffTime ),
                        backoffTime );
                }
            }

            /// <summary>
            /// Http get operation.
            /// </summary>
            /// <param name="url">The url.</param>
            /// <param name="parameters"><see cref="NameValueCollection"/> of the parameters</param>
            /// <returns><see cref="Models.HttpResponse"/></returns>
            private static Models.HttpResponse HttpGet( string url, NameValueCollection parameters )
            {
                var sw = new Stopwatch();

                try
                {
                    string response;

                    using ( var webClient = new Models.WebClientNoKeepAlive() )
                    {
                        sw.Start();
                        webClient.QueryString = parameters;
                        response = webClient.DownloadString( url );
                        sw.Stop();
                    }

                    if ( string.IsNullOrEmpty( response ) )
                        throw new Exception( "Invalid url" );

                    var obj = Configuration.JsonSerializer.Deserialize<dynamic>( response );

                    // The HTTP Response Status is only returned when there is
                    // an exception (i.e. it is not a 200), so we set it oursevles.
                    obj.StatusCode = HttpStatusCode.OK;

                    return new Models.HttpResponse
                    {
                        Response = obj,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
                catch ( WebException ex )
                {
                    if ( sw.IsRunning )
                        sw.Stop();

                    return new Models.HttpResponse
                    {
                        Response = ex.Response,
                        Exception = ex,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
                catch ( Exception ex )
                {
                    if ( sw.IsRunning )
                        sw.Stop();

                    return new Models.HttpResponse
                    {
                        Exception = ex,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
            }

            /// <summary>
            /// Http post object operation.
            /// </summary>
            /// <param name="url">The url.</param>
            /// <param name="data">Serialized data for the request.</param>
            /// <returns><see cref="Models.HttpResponse"/></returns>
            private static Models.HttpResponse HttpPostObject( string url, string data )
            {
                var sw = new Stopwatch();

                try
                {
                    string response;

                    using ( var webClient = new Models.WebClientNoKeepAlive() )
                    {
                        sw.Start();
                        response = webClient.UploadString( url, data );
                        sw.Stop();
                    }

                    if ( string.IsNullOrEmpty( response ) )
                        throw new Exception( "Invalid url" );

                    var obj = Configuration.JsonSerializer.Deserialize<dynamic>( response );

                    // The HTTP Response Status is only returned when there is
                    // an exception (i.e. it is not a 200), so we set it oursevles.
                    obj.StatusCode = HttpStatusCode.OK;

                    return new Models.HttpResponse
                    {
                        Response = obj,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
                catch ( WebException ex )
                {
                    if ( sw.IsRunning )
                        sw.Stop();

                    return new Models.HttpResponse
                    {
                        Response = ex.Response,
                        Exception = ex,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
                catch ( Exception ex )
                {
                    if ( sw.IsRunning )
                        sw.Stop();

                    return new Models.HttpResponse
                    {
                        Exception = ex,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
            }

            /// <summary>
            /// Http post parameters operation.
            /// </summary>
            /// <param name="url">The url.</param>
            /// <param name="parameters"><see cref="NameValueCollection"/> of the parameters</param>
            /// <returns><see cref="Models.HttpResponse"/></returns>
            private static Models.HttpResponse HttpPostParameters( string url, NameValueCollection parameters )
            {
                var sw = new Stopwatch();

                try
                {
                    string response;

                    using ( var webClient = new Models.WebClientNoKeepAlive() )
                    {
                        sw.Start();
                        response = Encoding.UTF8.GetString( webClient.UploadValues( url, parameters ) );
                        sw.Stop();
                    }

                    if ( string.IsNullOrEmpty( response ) )
                        throw new Exception( "Invalid url" );

                    var obj = Configuration.JsonSerializer.Deserialize<dynamic>( response );

                    // The HTTP Response Status is only returned when there is
                    // an exception (i.e. it is not a 200), so we set it oursevles.
                    obj.StatusCode = HttpStatusCode.OK;

                    return new Models.HttpResponse
                    {
                        Response = obj,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
                catch ( WebException ex )
                {
                    if ( sw.IsRunning )
                        sw.Stop();

                    return new Models.HttpResponse
                    {
                        Response = ex.Response,
                        Exception = ex,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
                catch ( Exception ex )
                {
                    if ( sw.IsRunning )
                        sw.Stop();

                    return new Models.HttpResponse
                    {
                        Exception = ex,
                        HttpRequestTimeInMilliseconds = sw.ElapsedMilliseconds
                    };
                }
            }

            /// <summary>
            /// Is the subscription for the given event.
            /// </summary>
            /// <param name="eventTopics">List of event topics.</param>
            /// <param name="subscriptionTopics">List of <see cref="Models.SubscriptionTopic"/></param>
            /// <returns></returns>
            private static bool IsSubscriptionForEvent( IDictionary<string, string> eventTopics,
                IEnumerable<Models.SubscriptionTopic> subscriptionTopics )
            {
                var matches = true;

                foreach ( var subscriptionTopic in subscriptionTopics )
                {
                    if ( !eventTopics.ContainsKey( subscriptionTopic.Key ) )
                    {
                        matches = false;
                        break;
                    }

                    var operatorString = subscriptionTopic.OperatorTypeCode.ToUpper();

                    if ( operatorString == Utils.Operator.Equal )
                    {
                        if ( eventTopics[ subscriptionTopic.Key ].ToUpper() != subscriptionTopic.Value.ToUpper() )
                        {
                            matches = false;
                            break;
                        }
                    }

                    if ( operatorString == Utils.Operator.Like )
                    {
                        if ( !eventTopics[ subscriptionTopic.Key ].Contains( subscriptionTopic.Value ) )
                        {
                            matches = false;
                            break;
                        }
                    }

                    if ( operatorString == Utils.Operator.In )
                    {
                        var subscriptionTopicValues = subscriptionTopic.Value.Split( ',' ).Select( v => v.Trim() );

                        if ( !subscriptionTopicValues.Contains( eventTopics[ subscriptionTopic.Key ] ) )
                        {
                            matches = false;
                            break;
                        }
                    }

                    if ( operatorString == Utils.Operator.NotIn )
                    {
                        var subscriptionTopicValues = subscriptionTopic.Value.Split( ',' ).Select( v => v.Trim() );

                        if ( subscriptionTopicValues.Contains( eventTopics[ subscriptionTopic.Key ] ) )
                        {
                            matches = false;
                            break;
                        }
                    }
                }

                return matches;
            }

            /// <summary>
            /// Log event subscription activity
            /// </summary>
            /// <param name="eventId">The id of the event associated with the activity.</param>
            /// <param name="eventSubscriptionId">The id of the event subscription associated with the activity.</param>
            /// <param name="activityTypeCode">The activity type code.</param>
            /// <param name="activityData">The activity data.</param>
            private static void LogEventSubscriptionActivity( object eventId, object eventSubscriptionId, string activityTypeCode, object activityData )
            {
                var activity = new Models.EventSubscriptionActivity
                {
                    EventId = eventId,
                    EventSubscriptionId = eventSubscriptionId,
                    ActivityTypeCode = activityTypeCode,
                    ActivityData = new { activityData, Configuration }
                }.Stamp();

                Configuration.Database().SaveEventSubscriptionActivity( activity );
            }

            /// <summary>
            /// Process the javascript transform for the subscription.
            /// </summary>
            /// <param name="transform">The javascript transform.</param>
            /// <param name="eventPayload">The request object.</param>
            /// <param name="topics">List of event topics.</param>
            /// <param name="requestType">The type of request you are making.</param>
            /// <returns><see cref="string"/></returns>
            private static string ProcessTransform( string transform, object eventPayload,
                IDictionary<string, string> topics, string requestType )
            {
                if ( eventPayload == null )
                {
                    eventPayload = new object();
                }

                // All curly brackets need to be escaped by doubling the brackets since
                // we are using String.Format
                const string script = @"

function isArray(object) {{
  var retVal = false;

  if (Object.prototype.toString.call(object) === '[object Array]') {{
    retVal = true;
  }}

  return retVal;
}}

function isObject(val) {{
  if (val === null) {{
    return false;
  }}

  return ((typeof val === 'function') || (typeof val === 'object'));
}}

function convertObjectToQueryString(obj) {{
  var str = '';

  for (var key in obj) {{
    if (str != '') {{
      str += '&';
    }}

    var value = obj[key];

    if (isArray(value) == true || isObject(value) == true) {{
      value = JSON.stringify(value); // JSON2 dependency
    }} else {{
      value = encodeURIComponent(value);
    }}

    str += key + '=' + value;
    }}

    return str;
}}

function convertObjectToNameValueCollection(obj) {{
  var fullStr = '';
  fullStr += '{{ ';

  for (var key in obj) {{
    var str = '';

    var value = obj[key];
        
    str += ""\"""" + key + ""\"""" + ':';
    str += JSON.stringify(value);
    str += ',';
    
fullStr += str;
  }}

  fullStr = fullStr.substring(0, fullStr.length-1);    

  fullStr += ' }}';
  
  return fullStr;
}}

function isEmptyObject(obj) {{
  for (var name in obj) {{
    return false;
  }}

  return true;
}}

function toDateString(date) {{
  return ToDateString(date.getFullYear(), date.getMonth() + 1, date.getDate());
}}

function addDays(date, numberOfDays) {{
  date.setDate(date.getDate() + numberOfDays);

  return date;
}}

function processTransform(eventData, topicData) {{
  var requestObj = JSON.parse(eventData);
  topicData = JSON.parse(topicData);

  {0}
  
  if ( requestType === 'OBJECT' ) {{
    return JSON.stringify(requestObj);
  }};

  if ( requestType === 'QUERY_STRING' ) {{
    return convertObjectToQueryString(requestObj);
  }};

  if ( requestType === 'PARAMETERS' ) {{
    return convertObjectToNameValueCollection(requestObj);
  }};
  
  return JSON.stringify(requestObj);
}};
";
                var engine = new Engine();

                var result = engine.Execute( string.Format( script, string.IsNullOrEmpty( transform ) ? string.Empty : transform ) )
                    .SetValue( "requestType", string.IsNullOrEmpty( requestType ) ? "OBJECT" : requestType )
                    .SetValue( "eventData", Configuration.JsonSerializer.Serialize( eventPayload ) )
                    .SetValue( "topicData", Configuration.JsonSerializer.Serialize( topics ) )
                    .SetValue( "ToDateString", new Func<int, int, int, string>( ToDateString ) )
                    .Execute( "processTransform(eventData, topicData)" )
                    .GetCompletionValue();

                return result.ToString();
            }
            
            /// <summary>
            /// Determine if the response was successful based on the api type.
            /// </summary>
            /// <param name="response">The <see cref="Models.HttpResponse"/></param>
            /// <param name="apiType">The type of the api.</param>
            /// <returns><see cref="bool"/></returns>
            private static bool ResponseIsSuccessful( Models.HttpResponse response, string apiType )
            {
                var input = new Models.WasApiCallSuccessfulInput
                {
                    HttpResponse = response,
                    ApiType = apiType
                };

                if ( Configuration.WasApiCallSuccessfulHandlers.ContainsKey( apiType ) )
                {
                    return Configuration.WasApiCallSuccessfulHandlers[ apiType ](
                         input
                         );
                }

                return Configuration.WasApiCallSuccessfulHandlers[ "DEFAULT" ](
                        input
                        );
            }

            /// <summary>
            /// Format the give date parts into a string.
            /// </summary>
            /// <param name="year">Year.</param>
            /// <param name="month">Month.</param>
            /// <param name="day">Day.</param>
            /// <returns><see cref="string"/></returns>
            private static string ToDateString( int year, int month, int day )
            {
                return new DateTime( year, month, day ).ToString( "yyyy-MM-dd" );
            }
            #endregion
        }

        /// <summary>
        /// All of the models used throughout spoke.
        /// </summary>
        public static class Models
        {
            /// <summary>
            /// Contains a list of activity.
            /// </summary>
            public class ActivityResponse
            {
                public List<dynamic> Activity;
            }

            /// <summary>
            /// Contains the basic fields used for auditing.
            /// </summary>
            public class Audit
            {
                public DateTime CreateDate;
                public string CreatedByApplication;
                public string CreatedByUser;
                public string CreatedByHostName;
            }

            /// <summary>
            /// Clock Event.
            /// </summary>
            public class ClockEvent
            {
                public int Year;
                public int Month;
                public int Day;
                public int Hour;
                public int Minute;
            }

            /// <summary>
            /// Input to the GetApiUri function in <see cref="SpokeConfiguration"/>.
            /// </summary>
            public class GetApiUriInput
            {
                public string Uri;

                public string ApiType;
            }

            /// <summary>
            /// Contains a list of Event Subscriptions.
            /// </summary>
            public class GetEventSubscriptionsResponse
            {
                public List<dynamic> EventSubscriptions;
            }

            /// <summary>
            /// Contains a subscription.
            /// </summary>
            public class GetSubscriptionResponse
            {
                public Subscription Subscription;
            }

            /// <summary>
            /// Contains a list of subscriptions.
            /// </summary>
            public class GetSubscriptionsResponse
            {
                public List<Subscription> Subscriptions;
            }

            /// <summary>
            /// Event.
            /// </summary>
            public class Event : Audit
            {
                public object EventId;
                public object EventPayload;
                public int TopicCount;
                public IDictionary<string, string> Topics;
                public List<EventTopic> EventTopics = new List<EventTopic>();
            }

            /// <summary>
            /// Contains a list of event names.
            /// </summary>
            public class EventNamesResponse
            {
                public List<string> EventNames;
            }

            /// <summary>
            /// EventTopic.
            /// </summary>
            public class EventTopic : Audit
            {
                public object EventTopicId;
                public object EventId;
                public string Key;
                public string Value;
            }

            /// <summary>
            /// Contains an <see cref="Event"/>
            /// </summary>
            public class EventResponse
            {
                public Event Event;
            }

            /// <summary>
            /// Contains a list of <see cref="Event"/>
            /// </summary>
            public class EventsResponse
            {
                public List<Event> Events;
            }

            /// <summary>
            /// EventSubscription.
            /// </summary>
            public class EventSubscription : Audit
            {
                public object EventSubscriptionId;
                public object EventId;
                public object SubscriptionId;
                public Event Event;
                public Subscription Subscription;
            }

            /// <summary>
            /// EventSubscriptionActivity.
            /// </summary>
            public class EventSubscriptionActivity : Audit
            {
                public object EventSubscriptionActivityId;
                public string ActivityTypeCode;
                public object EventId;
                public object EventSubscriptionId;
                public object ActivityData;
            }

            /// <summary>
            /// Contains the result of a function and/or the exception from that function.
            /// </summary>
            /// <typeparam name="T"></typeparam>
            public class ExceptionWrapperResult<T>
            {
                public T Result;
                public Exception Exception;
            }

            /// <summary>
            /// Http Response.
            /// </summary>
            public class HttpResponse
            {
                public dynamic Response;
                public Exception Exception;
                public long HttpRequestTimeInMilliseconds;
            }

            /// <summary>
            /// Mutex.
            /// </summary>
            public class Mutex
            {
                public object MutexId;
            }

            /// <summary>
            /// Contains a list of valid operator types codes.
            /// </summary>
            public class OperatorTypeCodesResponse
            {
                public List<string> ValidOperatorTypeCodes;
            }

            /// <summary>
            /// Contains the idea of the published event along with an event object.
            /// </summary>
            public class PublishEventResponse : EventResponse
            {
                public string EventId;
            }

            /// <summary>
            /// ProcessEventResponse.
            /// </summary>
            public class ProcessEventResponse
            {
                public dynamic ProcessEventResult;
            }

            /// <summary>
            /// Contains a list of valid service type codes.
            /// </summary>
            public class ServiceTypeCodesResponse
            {
                public List<string> ValidServiceTypeCodes;
            }

            /// <summary>
            /// Subscription.
            /// </summary>
            public class Subscription : Audit
            {
                public object SubscriptionId;
                public string SubscriptionName;
                public string SubscriptionStatusCode;
                public string ApiEndpoint;
                public string ApiType;
                public string HttpMethod;
                public string TransformFunction;
                public int? AbortAfterMinutes;
                public string RequestType;
                public List<SubscriptionTopic> Topics = new List<SubscriptionTopic>();
            }

            /// <summary>
            /// Contains a subscription.
            /// </summary>
            public class SubscriptionResponse
            {
                public Subscription Subscription;
            }

            /// <summary>
            /// Contains the key, value, and operator type for a subscription topic.
            /// </summary>
            public class SubscriptionTopic : Audit
            {
                public object SubscriptionTopicId;
                public string Key;
                public string Value;
                public string OperatorTypeCode;
            }

            /// <summary>
            /// Subscription notification.
            /// </summary>
            public class SubscriptionNotification
            {
                public EventSubscription EventSubscription;
                public string Uri;
                public object Payload;
                public DateTime LiveRetryExpirationTime;
            }

            /// <summary>
            /// Contains the different subscription status codes.
            /// </summary>
            public static class SubscriptionStatusCodes
            {
                public const string Active = "ACTIVE";
                public const string Inactive = "INACTIVE";
                public const string Deleted = "DELETED";
            }

            /// <summary>
            /// Contains the task configuration and a message.
            /// </summary>
            public class TaskResponse
            {
                public dynamic TaskConfiguration;

                public string Message;
            }

            /// <summary>
            /// Contains a list of topic keys.
            /// </summary>
            public class TopicKeysResponse
            {
                public List<string> TopicKeys;
            }

            /// <summary>
            /// The input for the WasApiCallSuccessful func in <see cref="SpokeConfiguration"/>
            /// </summary>
            public class WasApiCallSuccessfulInput
            {
                public HttpResponse HttpResponse;

                public string ApiType;
            }

            /// <summary>
            /// Custom <see cref="WebClient"/> that turns off keep alive by default.
            /// </summary>
            public class WebClientNoKeepAlive : WebClient
            {
                protected override WebRequest GetWebRequest( Uri address )
                {
                    var request = base.GetWebRequest( address );
                    if ( request is HttpWebRequest )
                    {
                        ( request as HttpWebRequest ).KeepAlive = false;
                    }
                    return request;
                }
            }
        }
        
        /// <summary>
        /// ISpokeDatabase, SQL database implementation
        /// </summary>
        // ReSharper disable once InconsistentNaming
        public static class DatabaseIO
        {
            /// <summary>
            /// ISpokeDatabase. Interface for database interaction.
            /// </summary>
            public interface ISpokeDatabase
            {
                /// <summary>
                /// Method for returning a unique list of event names.
                /// </summary>
                /// <returns>List of <see cref="string"/></returns>
                List<string> GetAllEventNames();

                /// <summary>
                /// Method for returning a unique list of topic keys.
                /// </summary>
                /// <returns>List of <see cref="string"/></returns>
                List<string> GetAllTopicKeys();

                /// <summary>
                /// Method for returning an event by id.
                /// </summary>
                /// <param name="eventId">Id of the event you want to retrieve.</param>
                /// <returns><see cref="object"/></returns>
                Models.Event GetEvent( object eventId );

                /// <summary>
                /// Method for returning some of the latest events.
                /// </summary>
                /// <param name="eventCount">The number of events to return</param>
                /// <param name="eventName">The name of the events you are looking for.</param>
                /// <param name="topicKey">The name of the topic you are looking for.</param>
                /// <returns>List of <see cref="Models.Event"/></returns>
                List<Models.Event> GetEvents(int? eventCount, string eventName, string topicKey);

                /// <summary>
                /// Method for returning some of the latest events.
                /// </summary>
                /// <param name="eventCount">The maximum number of events to search through</param>
                /// <param name="eventName">The name of the events you are looking for.</param>
                /// <param name="topicKey">The name of the topic you are looking for.</param>
                /// <returns>List of <see cref="Models.Event"/></returns>
                List<Models.Event> GetLatestEvents( int? eventCount, string eventName, string topicKey );

                /// <summary>
                /// Method for returning subscriptions related to an event.
                /// </summary>
                /// <param name="eventId">The id of the event you are referencing</param>
                /// <param name="getSubscriptionInformation">Detailed information about the subscriptions. True or False.</param>
                /// <returns>List of <see cref="Models.EventSubscription"/></returns>
                List<Models.EventSubscription> GetEventSubscriptions( object eventId, bool getSubscriptionInformation );

                /// <summary>
                /// Method for returning current activity for event subscriptions
                /// </summary>
                /// <param name="eventId">The id of the event being referenced.</param>
                /// <param name="subscriptionId">The id of the subscription being referenced.</param>
                /// <param name="activityCode">The name of the activity type you are looking for.</param>
                /// <param name="activityCount">The number of activity records you want to retrieve.</param>
                /// <returns>List of <see cref="Models.EventSubscriptionActivity"/></returns>
                List<Models.EventSubscriptionActivity> GetEventSubscriptionActivities( object eventId, object subscriptionId, string activityCode, int? activityCount );

                /// <summary>
                /// Method to retrieve a subscription
                /// </summary>
                /// <param name="subscriptionId">The id of the subscription being retrieved.</param>
                /// <param name="subscriptionName">The name of the subscription being retrieved</param>
                /// <returns><see cref="Models.Subscription"/></returns>
                Models.Subscription GetSubscription( object subscriptionId, string subscriptionName );

                /// <summary>
                /// Method to retrieve all subscriptions.
                /// </summary>
                /// <param name="activeOnly">Retrieve only active susbcriptions. True or False.</param>
                /// <returns>List of <see cref="Models.Subscription"/></returns>
                List<Models.Subscription> GetSubscriptions( bool activeOnly );

                /// <summary>
                /// Method to get failed events.
                /// </summary>
                /// <returns>List of <see cref="Models.Event"/></returns>
                List<Models.Event> GetFailedEvents();

                /// <summary>
                /// Method to get failed events subscription.
                /// </summary>
                /// <param name="lookbackMinutes">The number of minutes to look back.</param>
                /// <param name="lookbackUpToMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
                /// <returns>List of <see cref="Models.EventSubscription"/></returns>
                List<Models.EventSubscription> GetFailedEventSubscriptions( int? lookbackMinutes = null, int? lookbackUpToMinutes = null );

                /// <summary>
                /// Method to get missing clock events
                /// </summary>
                /// <param name="totalMinutes">The number of minutes to look back.</param>
                /// <param name="offsetMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
                /// <returns>List of <see cref="Models.ClockEvent"/></returns>
                List<Models.ClockEvent> GetMissingClockEvents( int? totalMinutes, int? offsetMinutes );

                /// <summary>
                /// Method to save an event.
                /// </summary>
                /// <param name="event"><see cref="Models.Event"/></param>
                /// <param name="saveTopics">Save event topics. True or False.</param>
                /// <returns><see cref="Models.Event"/></returns>
                Models.Event SaveEvent( Models.Event @event, bool saveTopics );

                /// <summary>
                /// Method for saving event topics.
                /// </summary>
                /// <param name="eventTopics">List of <see cref="Models.EventTopic"/></param>
                /// <returns>List of <see cref="Models.EventTopic"/></returns>
                List<Models.EventTopic> SaveEventTopics( List<Models.EventTopic> eventTopics );

                /// <summary>
                /// Method for saving event subscription activity.
                /// </summary>
                /// <param name="activity"><see cref="Models.EventSubscriptionActivity"/></param>
                /// <returns><see cref="Models.EventSubscriptionActivity"/></returns>
                Models.EventSubscriptionActivity SaveEventSubscriptionActivity( Models.EventSubscriptionActivity activity );

                /// <summary>
                /// Method for saving event subscriptions.
                /// </summary>
                /// <param name="eventSubscriptions">List of <see cref="Models.EventSubscription"/></param>
                /// <returns>List of <see cref="Models.EventSubscription"/></returns>
                List<Models.EventSubscription> SaveEventSubscriptions( List<Models.EventSubscription> eventSubscriptions );

                /// <summary>
                /// Method for saving a subscription
                /// </summary>
                /// <param name="subscription"><see cref="Models.Subscription"/></param>
                /// <returns><see cref="Models.Subscription"/></returns>
                Models.Subscription SaveSubscription( Models.Subscription subscription );

                /// <summary>
                /// Method for acquiring the active mutex.
                /// </summary>
                /// <param name="mutexKey">The mutex key trying to be retrieved.</param>
                /// <returns><see cref="Models.Mutex"/></returns>
                Models.Mutex GetActiveMutex( string mutexKey );

                /// <summary>
                /// Method for trying to acquire a mutex.
                /// </summary>
                /// <param name="mutexKey">The mutex key trying to be acquired.</param>
                /// <param name="timeToLive">The TimeSpan that the mutex should live.</param>
                /// <returns><see cref="Models.Mutex"/></returns>
                Models.Mutex TryAcquireMutex( string mutexKey, TimeSpan timeToLive );

                /// <summary>
                /// Method for releasing a mutex.
                /// </summary>
                /// <param name="mutex">The <see cref="Models.Mutex"/> you are trying to release.</param>
                void ReleaseMutex( Models.Mutex mutex );
            }

            /// <summary>
            /// Default SQL implementation.
            /// </summary>
            public class SpokeSqlDatabase : ISpokeDatabase
            {
                /// <summary>
                /// Method for returning a unique list of event names.
                /// </summary>
                /// <returns>List of <see cref="string"/></returns>
                public List<string> GetAllEventNames()
                {
                    var cmd = GetDbCommand()
                     .SetCommandText( @"
SELECT DISTINCT
    Value
FROM
    dbo.EventTopic
WHERE
    [Key] = 'EVENT_NAME'" );

                    var eventNames = cmd.ExecuteToList<string>();

                    return eventNames;
                }

                /// <summary>
                /// Method for returning a unique list of topic keys.
                /// </summary>
                /// <returns>List of <see cref="string"/></returns>
                public List<string> GetAllTopicKeys()
                {
                    var cmd = GetDbCommand()
                    .SetCommandText( @"
SELECT DISTINCT
    [Key]
FROM
    dbo.EventTopic" );

                    var topicKeys = cmd.ExecuteToList<string>();

                    return topicKeys;
                }

                /// <summary>
                /// Method for returning an event by id.
                /// </summary>
                /// <param name="eventId">Id of the event you want to retrieve.</param>
                /// <returns><see cref="object"/></returns>
                public Models.Event GetEvent( object eventId )
                {
                    var cmd = GetDbCommand()
                    .SetCommandText( @"
SELECT
    e.EventId
   ,e.EventData
   ,e.TopicData
   ,e.TopicCount
   ,e.CreatedByHostName AS CreatedByHostName
   ,e.CreatedByUser
   ,e.CreatedByApplication
   ,e.CreateDate
   ,et.EventTopicId
   ,et.[Key]
   ,et.Value
   ,et.CreatedByHostName AS TopicCreatedByHostName
   ,et.CreatedByUser AS TopicCreatedByUser
   ,et.CreatedByApplication AS TopicCreatedByApplication
   ,et.CreateDate AS TopicCreateDate
FROM
    dbo.Event e
    LEFT JOIN dbo.EventTopic et ON et.EventId = e.EventId
WHERE
    e.EventId = @eventId" )
                    .AddParameter( "@eventId", eventId, DbType.Int64 );

                    var result = cmd.ExecuteToDynamicList();

                    return ToEvent( result );
                }

                /// <summary>
                /// Method for returning the last N events.
                /// </summary>
                /// <param name="eventCount">The number of events to return</param>
                /// <param name="eventName">The name of the events you are looking for.</param>
                /// <param name="topicKey">The name of the topic you are looking for.</param>
                /// <returns>List of <see cref="Models.Event"/></returns>
                public List<Models.Event> GetEvents(int? eventCount, string eventName, string topicKey)
                {
                    var count = eventCount ?? 100;

                    var cmd = GetDbCommand()
                        .SetCommandText(@"
SELECT 
    E.EventId
   ,E.EventData
   ,E.TopicData
   ,E.TopicCount
   ,E.CreatedByHostName AS CreatedByHostName
   ,E.CreatedByUser
   ,E.CreatedByApplication
   ,E.CreateDate
   ,et.EventTopicId
   ,et.[Key]
   ,et.Value
   ,et.CreatedByHostName AS TopicCreatedByHostName
   ,et.CreatedByUser AS TopicCreatedByUser
   ,et.CreatedByApplication AS TopicCreatedByApplication
   ,et.CreateDate AS TopicCreateDate
FROM
    dbo.[Event] E
    LEFT JOIN dbo.EventTopic et ON et.EventId = e.EventId
    {0}
ORDER BY
    E.EventId DESC")
                        //.AddParameter("@count", count, DbType.Int32)
                        ;

                    var joins = string.Empty;
                    if (!string.IsNullOrEmpty(eventName))
                    {
                        joins += @"
INNER JOIN dbo.EventTopic T1 ON T1.EventId = E.EventId
                                AND T1.[Key] = 'EVENT_NAME'
                                AND T1.Value = @eventName ";
                        cmd.AddParameter("@eventName", eventName, DbType.AnsiString);
                    }
                    if (!string.IsNullOrEmpty(topicKey))
                    {
                        joins += @"
INNER JOIN dbo.EventTopic T2 ON T2.EventId = E.EventId
                                AND T2.[Key] = @topicKey";
                        cmd.AddParameter("@topicKey", topicKey, DbType.AnsiString);
                    }

                    cmd.SetCommandText(string.Format(cmd.DbCommand.CommandText, joins));

                    var dbResult = cmd.ExecuteToDynamicList();

                    var distinctEvents = new Dictionary<long, List<dynamic>>();

                    foreach (var @event in dbResult)
                    {
                        var eventId = Convert.ToInt64(@event.EventId);

                        if (distinctEvents.ContainsKey(eventId))
                            distinctEvents[eventId].Add(@event);
                        else
                            distinctEvents.Add(eventId, new List<dynamic> { @event });

                        if (distinctEvents.Count >= count)
                        {
                            break;
                        }
                    }

                    var events = new ConcurrentBag<Models.Event>();

                    Parallel.ForEach(distinctEvents, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                        @event => events.Add(ToEvent(@event.Value)));

                    return events.OrderByDescending(x => x.EventId).ToList();
                }

                /// <summary>
                /// Method for returning some of the latest events.
                /// </summary>
                /// <param name="eventCount">The maximum number of events to search through.</param>
                /// <param name="eventName">The name of the events you are looking for.</param>
                /// <param name="topicKey">The name of the topic you are looking for.</param>
                /// <returns>List of <see cref="Models.Event"/></returns>
                public List<Models.Event> GetLatestEvents( int? eventCount, string eventName, string topicKey )
                {
                    var count = eventCount ?? 100;

                    var cmd = GetDbCommand()
                        .SetCommandText( @"
SELECT
    E.EventId
   ,E.EventData
   ,E.TopicData
   ,E.TopicCount
   ,E.CreatedByHostName AS CreatedByHostName
   ,E.CreatedByUser
   ,E.CreatedByApplication
   ,E.CreateDate
   ,et.EventTopicId
   ,et.[Key]
   ,et.Value
   ,et.CreatedByHostName AS TopicCreatedByHostName
   ,et.CreatedByUser AS TopicCreatedByUser
   ,et.CreatedByApplication AS TopicCreatedByApplication
   ,et.CreateDate AS TopicCreateDate
FROM
    dbo.[Event] E
    LEFT JOIN dbo.EventTopic et ON et.EventId = e.EventId
    {0}
WHERE
    E.EventId IN (
        SELECT TOP ( @count )
            EventId
        FROM
            dbo.Event
        ORDER BY 
            EventId DESC )
ORDER BY
    E.EventId DESC" )
                        .AddParameter( "@count", count, DbType.Int32 );

                    var joins = string.Empty;
                    if ( !string.IsNullOrEmpty( eventName ) )
                    {
                        joins += @"
INNER JOIN dbo.EventTopic T1 ON T1.EventId = E.EventId
                                AND T1.[Key] = 'EVENT_NAME'
                                AND T1.Value = @eventName ";
                        cmd.AddParameter( "@eventName", eventName, DbType.AnsiString );
                    }
                    if ( !string.IsNullOrEmpty( topicKey ) )
                    {
                        joins += @"
INNER JOIN dbo.EventTopic T2 ON T2.EventId = E.EventId
                                AND T2.[Key] = @topicKey";
                        cmd.AddParameter( "@topicKey", topicKey, DbType.AnsiString );
                    }

                    cmd.SetCommandText( string.Format( cmd.DbCommand.CommandText, joins ) );

                    var dbResult = cmd.ExecuteToDynamicList();

                    var distinctEvents = new Dictionary<long, List<dynamic>>();

                    foreach ( var @event in dbResult )
                    {
                        var eventId = Convert.ToInt64( @event.EventId );

                        if ( distinctEvents.ContainsKey( eventId ) )
                            distinctEvents[ eventId ].Add( @event );
                        else
                            distinctEvents.Add( eventId, new List<dynamic> { @event } );
                    }

                    var events = new ConcurrentBag<Models.Event>();

                    Parallel.ForEach( distinctEvents, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                        @event => events.Add( ToEvent( @event.Value ) ) );

                    return events.OrderByDescending( x => x.EventId ).ToList();
                }

                /// <summary>
                /// Method for returning subscriptions related to an event.
                /// </summary>
                /// <param name="eventId">The id of the event you are referencing</param>
                /// <param name="getSubscriptionInformation">Detailed information about the subscriptions. True or False.</param>
                /// <returns>List of <see cref="Models.EventSubscription"/></returns>
                public List<Models.EventSubscription> GetEventSubscriptions( object eventId, bool getSubscriptionInformation )
                {
                    var cmd = GetDbCommand()
                    .SetCommandText( @"
SELECT
    EventSubscriptionId
    ,EventId
    ,SubscriptionId
    ,CreatedByHostName AS CreatedByHostName
    ,CreateDate
    ,CreatedByUser
    ,CreatedByApplication
FROM
    dbo.EventSubscription
WHERE
    EventId = @eventId
" )
                    .AddParameter( "@eventId", eventId, DbType.Int64 );

                    var eventSubscriptions = cmd.ExecuteToList<Models.EventSubscription>();

                    if ( getSubscriptionInformation )
                    {
                        var subscriptions = GetSubscriptions( false );

                        foreach ( var eventSubscription in eventSubscriptions )
                        {
                            var subscription = subscriptions.FirstOrDefault( x => x.SubscriptionId.ToInt() == eventSubscription.SubscriptionId.ToInt() );

                            eventSubscription.Subscription = subscription;
                        }
                    }

                    return eventSubscriptions;
                }

                /// <summary>
                /// Method for returning current activity for event subscriptions
                /// </summary>
                /// <param name="eventId">The id of the event being referenced.</param>
                /// <param name="subscriptionId">The id of the subscription being referenced.</param>
                /// <param name="activityCode">The name of the activity type you are looking for.</param>
                /// <param name="activityCount">The number of activity records you want to retrieve.</param>
                /// <returns>List of <see cref="Models.EventSubscriptionActivity"/></returns>
                public List<Models.EventSubscriptionActivity> GetEventSubscriptionActivities( object eventId, object subscriptionId, string activityCode, int? activityCount )
                {
                    var cmd = GetDbCommand()
                    .SetCommandText( @"
SELECT {0}
    A.EventSubscriptionActivityId
   ,A.ActivityTypeCode
   ,A.EventId
   ,A.EventSubscriptionId
   ,ES.SubscriptionId
   ,A.Data AS ActivityData
   ,A.CreatedByHostName AS CreatedByHostName
   ,A.CreateDate
   ,A.CreatedByUser
   ,A.CreatedByApplication
FROM
    dbo.EventSubscriptionActivity A
    LEFT JOIN dbo.EventSubscription ES ON ES.EventSubscriptionId = A.EventSubscriptionId
{1}
ORDER BY
    A.EventSubscriptionActivityId DESC" );

                    var topN = string.Empty;
                    activityCount = activityCount.HasValue
                        ? activityCount
                        : ( eventId == null || subscriptionId == null ) ? 100 : new int?();
                    if ( activityCount.HasValue )
                    {
                        topN += "TOP ( @count )";
                        cmd.AddParameter( "@count", activityCount.Value, DbType.Int32 );
                    }

                    var conditional = string.Empty;
                    if ( eventId != null )
                    {
                        conditional += " WHERE A.EventId = @eventId ";
                        cmd.AddParameter( "@eventId", eventId, DbType.Int64 );
                    }
                    if ( !string.IsNullOrEmpty( activityCode ) )
                    {
                        conditional += string.IsNullOrEmpty( conditional )
                            ? " WHERE A.ActivityTypeCode = @activityCode "
                            : " AND A.ActivityTypeCode = @activityCode ";

                        cmd.AddParameter( "@activityCode", activityCode, DbType.AnsiString );
                    }
                    if ( subscriptionId != null )
                    {
                        conditional += string.IsNullOrEmpty( conditional )
                            ? " WHERE ES.SubscriptionId = @subscriptionId "
                            : " AND ES.SubscriptionId = @subscriptionId ";

                        cmd.AddParameter( "@subscriptionId", subscriptionId, DbType.Int32 );
                    }

                    cmd.SetCommandText( string.Format( cmd.DbCommand.CommandText, topN, conditional ) );

                    return cmd.ExecuteToList<Models.EventSubscriptionActivity>();
                }

                /// <summary>
                /// Method to retrieve a subscription
                /// </summary>
                /// <param name="subscriptionId">The id of the subscription being retrieved.</param>
                /// <param name="subscriptionName">The name of the subscription being retrieved</param>
                /// <returns><see cref="Models.Subscription"/></returns>
                public Models.Subscription GetSubscription( object subscriptionId, string subscriptionName )
                {
                    return GetSubscriptions( null, subscriptionId != null ? Convert.ToInt32( subscriptionId ) : new int?(),
                        subscriptionName ).FirstOrDefault();
                }

                /// <summary>
                /// Method to retrieve all subscriptions.
                /// </summary>
                /// <param name="activeOnly">Retrieve only active susbcriptions. True or False.</param>
                /// <returns>List of <see cref="Models.Subscription"/></returns>
                public List<Models.Subscription> GetSubscriptions( bool activeOnly )
                {
                    return GetSubscriptions( activeOnly, null, null );
                }

                /// <summary>
                /// Method to get failed events.
                /// </summary>
                /// <returns>List of <see cref="Models.Event"/></returns>
                public List<Models.Event> GetFailedEvents()
                {
                    var cmd = GetDbCommand().SetCommandText( @"
SELECT DISTINCT
    E.EventId
FROM
    dbo.[Event] E
    LEFT JOIN dbo.EventSubscriptionActivity A ON A.EventId = E.EventId
                                                 AND ActivityTypeCode = 'SUBSCRIPTIONS_FOUND'
    LEFT JOIN ( SELECT
                    E.EventId
                   ,COUNT(*) AS TopicCount
                FROM
                    dbo.[Event] E
                    JOIN dbo.EventTopic T ON T.EventId = E.EventId
                GROUP BY
                    E.EventId
              ) TC ON TC.EventId = E.EventId
WHERE
    (
        A.EventId IS NULL
        OR E.TopicCount > TC.TopicCount
    )
    AND E.CreateDate > @startDate
    AND E.CreateDate <= @endDate
" );

                    cmd.AddParameter( "@startDate", DateTime.Now.AddMinutes( -1 * Configuration.FailedEventsLookbackMinutes ?? 0 ), DbType.DateTime )
                       .AddParameter( "@endDate", DateTime.Now.AddMinutes( -1 * Configuration.FailedEventsThresholdMinutes ?? 0 ), DbType.DateTime );

                    var eventIds = cmd.ExecuteToList<long>();

                    var events = new ConcurrentBag<Models.Event>();

                    Parallel.ForEach( eventIds, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                        eventId => events.Add( GetEvent( eventId ) ) );

                    return events.OrderBy( x => x.EventId ).ToList();
                }

                /// <summary>
                /// Method to get failed events subscription.
                /// </summary>
                /// <param name="lookbackMinutes">The number of minutes to look back.</param>
                /// <param name="lookbackUpToMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
                /// <returns>List of <see cref="Models.EventSubscription"/></returns>
                public List<Models.EventSubscription> GetFailedEventSubscriptions( int? lookbackMinutes = null, int? lookbackUpToMinutes = null )
                {
                    var cmd = GetDbCommand()
                    .SetCommandText( @"
SELECT
    ES.EventSubscriptionId
   ,ES.CreatedByHostName AS EsCreatedByHostName
   ,ES.CreatedByUser AS EsCreatedByUser
   ,ES.CreatedByApplication AS EsCreatedByApplication
   ,ES.CreateDate AS EsCreateDate
   ,E.EventId
   ,S.SubscriptionId
   ,E.EventData
   ,E.TopicData
   ,E.TopicCount
   ,E.CreatedByHostName AS CreatedByHostName
   ,E.CreatedByUser
   ,E.CreatedByApplication
   ,E.CreateDate
   ,ET.EventTopicId
   ,ET.[Key]
   ,ET.Value
   ,ET.CreatedByHostName AS TopicCreatedByHostName
   ,ET.CreatedByUser AS TopicCreatedByUser
   ,ET.CreatedByApplication AS TopicCreatedByApplication
   ,ET.CreateDate AS TopicCreateDate
FROM
    dbo.[Event] E
    INNER JOIN dbo.EventSubscription ES ON ES.EventId = E.EventId
    INNER JOIN dbo.Subscription S ON S.SubscriptionId = ES.SubscriptionId
    INNER JOIN dbo.SubscriptionRevision R ON R.SubscriptionRevisionId = S.CurrentSubscriptionRevisionId
    LEFT JOIN dbo.EventTopic et ON et.EventId = E.EventId
    LEFT JOIN dbo.EventSubscriptionActivity A ON A.EventId = E.EventId
                                                 AND A.EventSubscriptionId = ES.EventSubscriptionId
                                                 AND ActivityTypeCode = 'SUBSCRIPTION_RESPONSE_OK'
WHERE
    A.EventSubscriptionActivityId IS NULL
    AND E.CreateDate > @startDate
    AND E.CreateDate <= @endDate
    AND GETDATE() <= DATEADD(Minute, ISNULL(R.AbortAfterMinutes, @abortMinutes), E.CreateDate)
" )
                    .AddParameter( "@endDate",
                        DateTime.Now.AddMinutes( -1 * lookbackUpToMinutes ?? Configuration.FailedNotificationsThresholdMinutes ?? 0 ),
                        DbType.DateTime )
                    .AddParameter( "@startDate", DateTime.Now.AddMinutes( -1 * lookbackMinutes ?? Configuration.LiveRetryAbortAfterMinutes ?? 0 ),
                        DbType.DateTime )
                    .AddParameter( "@abortMinutes", Configuration.DefaultAbortAfterMinutes ?? 0, DbType.Int32 );

                    var dbResult = cmd.ExecuteToDynamicList();

                    if ( !dbResult.Any() )
                        return new List<Models.EventSubscription>();

                    var eventSubscriptions = new ConcurrentBag<Models.EventSubscription>();

                    Parallel.ForEach( dbResult.Select( x => x.EventId ).Distinct(),
                        new ParallelOptions { MaxDegreeOfParallelism = 8 },
                        x =>
                        {
                            var data = dbResult.Where( y => y.EventId == x ).ToList();

                            foreach ( var subscription in data.Select( z => z.SubscriptionId ).Distinct() )
                            {
                                var subscriptionData = data.Where( d => d.SubscriptionId == subscription ).ToList();

                                var first = subscriptionData.First();

                                eventSubscriptions.Add( new Models.EventSubscription
                                {
                                    EventSubscriptionId = first.EventSubscriptionId,
                                    EventId = first.EventId,
                                    SubscriptionId = first.SubscriptionId,
                                    CreatedByHostName = first.EsCreatedByHostName,
                                    CreateDate = first.EsCreateDate,
                                    CreatedByApplication = first.EsCreatedByApplication,
                                    CreatedByUser = first.EsCreatedByUser,
                                    Event = ToEvent( subscriptionData )
                                } );
                            }
                        }
                    );

                    var subscriptions = GetSubscriptions( true, null, null );

                    Parallel.ForEach( eventSubscriptions, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                        x => x.Subscription = subscriptions.FirstOrDefault( s => Convert.ToInt32( s.SubscriptionId ) == Convert.ToInt32( x.SubscriptionId ) ) );

                    return eventSubscriptions.ToList();
                }

                /// <summary>
                /// Underlying method for getsubscription and getsubscriptions.
                /// </summary>
                /// <param name="activeOnly">Retrieve only active subscriptions. True or False.</param>
                /// <param name="subscriptionId">The Id of the subscription are you are trying to retrieve.</param>
                /// <param name="subscriptionName">The name of the subscription you are trying to retrieve.</param>
                /// <returns>List of <see cref="Models.Subscription"/></returns>
                private static List<Models.Subscription> GetSubscriptions( bool? activeOnly, int? subscriptionId, string subscriptionName )
                {
                    var cmd = GetDbCommand()
                        .SetCommandText( @"
SELECT
    S.SubscriptionId
   ,R.SubscriptionName
   ,R.SubscriptionRevisionId
   ,R.SubscriptionStatusCode
   ,R.ServiceEndpoint
   ,R.HTTPMethod
   ,R.ServiceTypeCode
   ,R.TransformFunction
   ,R.AbortAfterMinutes
   ,R.RequestType
   ,T.[Key]
   ,T.Value
   ,T.OperatorTypeCode
   ,R.CreateDate
   ,R.CreatedByApplication
   ,R.CreatedByUser
   ,R.CreatedByHostName AS CreatedByHostName
   ,T.CreateDate AS TopicCreateDate
   ,T.CreatedByApplication AS TopicCreatedByApplication
   ,T.CreatedByUser AS TopicCreatedByUser
   ,T.CreatedByHostName AS TopicCreatedByHostName
FROM
    dbo.Subscription S
    INNER JOIN dbo.SubscriptionRevision R ON R.SubscriptionId = S.SubscriptionId
                                             AND R.SubscriptionRevisionId = S.CurrentSubscriptionRevisionId
    INNER JOIN dbo.SubscriptionTopic T ON T.SubscriptionRevisionId = R.SubscriptionRevisionId
WHERE
    1=1
    {0}" );

                    var conditional = string.Empty;
                    if ( subscriptionId.HasValue )
                    {
                        conditional += " AND S.SubscriptionId = @subscriptionId ";
                        cmd.AddParameter( "@subscriptionId", subscriptionId, DbType.Int32 );
                    }
                    if ( !string.IsNullOrEmpty( subscriptionName ) )
                    {
                        conditional += " AND R.SubscriptionName = @subscriptionName ";
                        cmd.AddParameter( "@subscriptionName", subscriptionName, DbType.AnsiString );
                    }

                    cmd.SetCommandText( string.Format( cmd.DbCommand.CommandText, conditional ) );

                    var results = cmd.ExecuteToDynamicList();

                    // Group all topics from the same revision together.
                    var revisionGroupings = results.GroupBy(
                        x => new
                        {
                            x.SubscriptionId,
                            x.SubscriptionName,
                            x.SubscriptionRevisionId,
                            x.SubscriptionStatusCode,
                            x.ServiceEndpoint,
                            x.HTTPMethod,
                            x.ServiceTypeCode,
                            x.TransformFunction,
                            x.AbortAfterMinutes,
                            x.RequestType
                        } );

                    // Project the results into a Subscription object which contains a list of
                    // SubscriptionTopics.
                    var subscriptions = revisionGroupings.Select(
                        revision => new Models.Subscription
                        {
                            SubscriptionId = revision.Key.SubscriptionId,
                            SubscriptionName = revision.Key.SubscriptionName,
                            SubscriptionStatusCode = revision.Key.SubscriptionStatusCode,
                            ApiEndpoint = revision.Key.ServiceEndpoint,
                            HttpMethod = revision.Key.HTTPMethod,
                            ApiType = revision.Key.ServiceTypeCode,
                            TransformFunction = revision.Key.TransformFunction,
                            AbortAfterMinutes = revision.Key.AbortAfterMinutes,
                            RequestType = revision.Key.RequestType,
                            CreateDate = revision.Last().CreateDate,
                            CreatedByApplication = revision.Last().CreatedByApplication,
                            CreatedByUser = revision.Last().CreatedByUser,
                            CreatedByHostName = revision.Last().CreatedByHostName,
                            Topics = revision.Distinct( new SubscriptionTopicComparer<dynamic>() )
                                .Select( t => new Models.SubscriptionTopic
                                {
                                    Key = t.Key,
                                    Value = t.Value,
                                    OperatorTypeCode = t.OperatorTypeCode,
                                    CreateDate = t.TopicCreateDate,
                                    CreatedByApplication = t.TopicCreatedByApplication,
                                    CreatedByUser = t.TopicCreatedByUser,
                                    CreatedByHostName = t.TopicCreatedByHostName
                                } ).ToList()
                        } );

                    if ( activeOnly.HasValue )
                    {
                        subscriptions = subscriptions.Where( x => activeOnly.Value
                            ? x.SubscriptionStatusCode == Models.SubscriptionStatusCodes.Active
                            : x.SubscriptionStatusCode != Models.SubscriptionStatusCodes.Deleted );
                    }

                    return subscriptions.ToList();
                }

                /// <summary>
                /// Method to get missing clock events
                /// </summary>
                /// <param name="totalMinutes">The number of minutes to look back.</param>
                /// <param name="offsetMinutes">The number of buffer minutes between now and the end of the time frame you are looking in.</param>
                /// <returns>List of <see cref="Models.ClockEvent"/></returns>
                public List<Models.ClockEvent> GetMissingClockEvents(
                    int? totalMinutes,
                    int? offsetMinutes
                    )
                {
                    const string query = @"
IF OBJECT_ID('tempdb..#tmp') IS NOT NULL
  DROP TABLE #tmp

CREATE TABLE #tmp
(
	 Offset int
	, OffsetDate datetime
)

INSERT INTO #tmp
SELECT TOP (@TotalMinutes)
  ROW_NUMBER() OVER (ORDER BY C1.id)
  , NULL
FROM          syscolumns AS C1
CROSS JOIN    syscolumns AS C2

UPDATE #tmp 
SET
	Offset = Offset
	, OffsetDate = (DATEADD(MINUTE, -Offset, @EndDate))
FROM #tmp

IF OBJECT_ID('tempdb..#tmp2') IS NOT NULL
  DROP TABLE #tmp2

CREATE TABLE #tmp2
(
	 EventId INT
	, [Year] VARCHAR(100)
	, [Month] VARCHAR(100)
	, [Day] VARCHAR(100)
	, [Hour] VARCHAR(100)
	, [Minute] VARCHAR(100)
)

DECLARE @StartDate DATETIME = (SELECT DATEADD(MINUTE, -1, MIN(OffsetDate)) FROM #tmp)

INSERT INTO #tmp2
SELECT 
	E.EventId
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
FROM dbo.[Event] E (NOLOCK)
JOIN dbo.EventTopic T (NOLOCK)
ON T.EventId = E.EventId
WHERE E.CreateDate > @StartDate
  AND T.[Key] = 'EVENT_NAME'
  AND T.Value = 'ClockEvent'

IF OBJECT_ID('tempdb..#tmp3') IS NOT NULL
  DROP TABLE #tmp3

CREATE TABLE #tmp3
(
	 EventId INT
	, [Key] VARCHAR(100)
	, [Value] VARCHAR(100)
)

INSERT INTO #tmp3
SELECT
  E.EventId
  , [Key]
  , [Value]
FROM dbo.EventTopic T (NOLOCK)
JOIN #tmp2 E
ON E.EventId = T.EventId

UPDATE #tmp2
SET [Year] = T1.Value
, [Month] = T2.value
, [Day] = T3.value
, [Hour] = T4.value
, [Minute] = T5.Value
FROM #tmp2 E
JOIN #tmp3 T1 (NOLOCK)
ON T1.EventId = E.EventId
  AND T1.[Key] = 'Year'
JOIN #tmp3 T2 (NOLOCK)
ON T2.EventId = E.EventId
  AND T2.[Key] = 'Month'
JOIN #tmp3 T3 (NOLOCK)
ON T3.EventId = E.EventId
  AND T3.[Key] = 'Day'
JOIN #tmp3 T4 (NOLOCK)
ON T4.EventId = E.EventId
  AND T4.[Key] = 'Hour'
JOIN #tmp3 T5 (NOLOCK)
ON T5.EventId = E.EventId
  AND T5.[Key] = 'Minute'
 
SELECT
  D.[Year]
  , D.[Month]
  , D.[Day]
  , D.[Hour]
  , D.[Minute]
FROM
(SELECT OffsetDate
 , CAST(DATEPART(YEAR, OffsetDate) AS VARCHAR) AS [Year]
 , CAST(DATEPART(MONTH, OffsetDate) AS VARCHAR) AS [Month]
 , CAST(DATEPART(DAY, OffsetDate) AS VARCHAR) AS [Day]
 , CAST(DATEPART(HOUR, OffsetDate) AS VARCHAR) AS [Hour]
 , CAST(DATEPART(MINUTE, OffsetDate) AS VARCHAR) AS [Minute]
 FROM #tmp) D
LEFT JOIN
#tmp2 T
ON T.[Year] = D.[Year]
  AND T.[Month] = D.[Month]
  AND T.[Day] = D.[Day]
  AND T.[Hour] = D.[Hour]
  AND T.[Minute] = D.[Minute]
WHERE T.[Minute] IS NULL

IF OBJECT_ID('tempdb..#tmp') IS NOT NULL
  DROP TABLE #tmp
IF OBJECT_ID('tempdb..#tmp2') IS NOT NULL
  DROP TABLE #tmp2
IF OBJECT_ID('tempdb..#tmp3') IS NOT NULL
  DROP TABLE #tmp3
";
                    var cmd = GetDbCommand()
                        .SetCommandText( query )
                        .AddParameter( "@TotalMinutes", totalMinutes ?? Configuration.ClockBackfillTotalMinutes ?? 0, DbType.Int32 )
                        .AddParameter( "@EndDate", DateTime.Now.AddMinutes( -1 * ( offsetMinutes ?? Configuration.ClockBackfillOffsetMinutes ?? 0 ) ), DbType.DateTime );

                    var missingEvents = cmd.ExecuteToList<Models.ClockEvent>();

                    return missingEvents;
                }

                /// <summary>
                /// Method to save an event.
                /// </summary>
                /// <param name="event"><see cref="Models.Event"/></param>
                /// <param name="saveTopics">Save event topics. True or False.</param>
                /// <returns><see cref="Models.Event"/></returns>
                public Models.Event SaveEvent( Models.Event @event, bool saveTopics )
                {
                    var obj = new
                    {
                        EventData = Configuration.JsonSerializer.Serialize( @event.EventPayload ),
                        TopicData = Configuration.JsonSerializer.Serialize( @event.Topics ),
                        @event.TopicCount,
                        @event.CreateDate,
                        @event.CreatedByApplication,
                        @event.CreatedByUser
                    };

                    var cmd = GetDbCommand()
                        .GenerateInsertForSqlServer( obj, "dbo.Event" );

                    @event.EventId = Convert.ToInt64( cmd.ExecuteScalar() );

                    if ( saveTopics )
                    {
                        @event.EventTopics.ForEach( x => x.EventId = @event.EventId );

                        @event.EventTopics = SaveEventTopics( @event.EventTopics );
                    }

                    return @event;
                }

                /// <summary>
                /// Method for saving event topics.
                /// </summary>
                /// <param name="eventTopics">List of <see cref="Models.EventTopic"/></param>
                /// <returns>List of <see cref="Models.EventTopic"/></returns>
                public List<Models.EventTopic> SaveEventTopics( List<Models.EventTopic> eventTopics )
                {
                    if ( !eventTopics.Any() )
                        return eventTopics;

                    var cmd = GetDbCommand()
                        .GenerateInsertsForSqlServer( eventTopics, "dbo.EventTopic" );

                    var ids = cmd.ExecuteToList<long>();

                    for ( var i = 0; i < ids.Count; i++ )
                    {
                        eventTopics[ i ].EventTopicId = ids[ i ];
                    }

                    return eventTopics;
                }

                /// <summary>
                /// Method for saving event subscription activity.
                /// </summary>
                /// <param name="activity"><see cref="Models.EventSubscriptionActivity"/></param>
                /// <returns><see cref="Models.EventSubscriptionActivity"/></returns>
                public Models.EventSubscriptionActivity SaveEventSubscriptionActivity( Models.EventSubscriptionActivity activity )
                {
                    var cmd = GetDbCommand()
                    .GenerateInsertForSqlServer( new
                    {
                        activity.ActivityTypeCode,
                        activity.EventId,
                        activity.EventSubscriptionId,
                        Data = Configuration.JsonSerializer.Serialize( activity.ActivityData ),
                        activity.CreateDate,
                        activity.CreatedByApplication,
                        activity.CreatedByUser
                    }, "dbo.EventSubscriptionActivity" );

                    activity.EventSubscriptionActivityId = Convert.ToInt64( cmd.ExecuteScalar() );

                    return activity;
                }
                /// <summary>
                /// Method for saving event subscriptions.
                /// </summary>
                /// <param name="eventSubscriptions">List of <see cref="Models.EventSubscription"/></param>
                /// <returns>List of <see cref="Models.EventSubscription"/></returns>
                public List<Models.EventSubscription> SaveEventSubscriptions( List<Models.EventSubscription> eventSubscriptions )
                {
                    if ( !eventSubscriptions.Any() )
                        return eventSubscriptions;

                    var cmd = GetDbCommand()
                        .GenerateInsertsForSqlServer( eventSubscriptions.Select( x =>
                            new
                            {
                                x.EventId,
                                x.SubscriptionId,
                                x.CreateDate,
                                x.CreatedByApplication,
                                x.CreatedByUser
                            } ).ToList(), "dbo.EventSubscription" );

                    var ids = cmd.ExecuteToList<long>();

                    for ( var i = 0; i < ids.Count; i++ )
                    {
                        eventSubscriptions[ i ].EventSubscriptionId = ids[ i ];
                    }

                    return eventSubscriptions;
                }

                /// <summary>
                /// Method for saving a subscription
                /// </summary>
                /// <param name="subscription"><see cref="Models.Subscription"/></param>
                /// <returns><see cref="Models.Subscription"/></returns>
                public Models.Subscription SaveSubscription( Models.Subscription subscription )
                {
                    DatabaseCommand cmd;
                    if ( subscription.SubscriptionId == null )
                    {
                        cmd = GetDbCommand()
                            .GenerateInsertForSqlServer( new
                            {
                                CurrentSubscriptionRevisionId = 0,
                                CreatedByHostName = subscription.CreatedByHostName,
                                subscription.CreateDate,
                                subscription.CreatedByUser,
                                subscription.CreatedByApplication
                            }, "dbo.Subscription" );

                        subscription.SubscriptionId = cmd.ExecuteScalar<int>();
                    }

                    cmd = GetDbCommand()
                        .GenerateInsertForSqlServer( new
                        {
                            subscription.SubscriptionId,
                            subscription.SubscriptionName,
                            subscription.SubscriptionStatusCode,
                            ServiceEndpoint = subscription.ApiEndpoint,
                            ServiceTypeCode = subscription.ApiType,
                            HTTPMethod = subscription.HttpMethod,
                            subscription.TransformFunction,
                            AbortAfterMinutes = subscription.AbortAfterMinutes ?? Configuration.LiveRetryAbortAfterMinutes,
                            subscription.RequestType,
                            CreatedByHostName = subscription.CreatedByHostName,
                            subscription.CreateDate,
                            subscription.CreatedByUser,
                            subscription.CreatedByApplication
                        }, "dbo.SubscriptionRevision" );

                    var revisionId = cmd.ExecuteScalar<int>();

                    cmd = GetDbCommand()
                        .SetCommandText( @"
UPDATE
    dbo.Subscription
SET
    CurrentSubscriptionRevisionId = @revisionId
WHERE
    SubscriptionId = @subscriptionId
" )
                        .AddParameter( "@revisionId", revisionId, DbType.Int32 )
                        .AddParameter( "@subscriptionId", subscription.SubscriptionId, DbType.Int32 );

                    cmd.ExecuteNonQuery();

                    cmd = GetDbCommand()
                        .GenerateInsertsForSqlServer( subscription.Topics.Select( x => new
                        {
                            SubscriptionRevisionId = revisionId,
                            x.Key,
                            x.Value,
                            x.OperatorTypeCode,
                            CreatedByHostName = x.CreatedByHostName,
                            x.CreateDate,
                            x.CreatedByUser,
                            x.CreatedByApplication
                        } ).ToList(), "SubscriptionTopic" );

                    var ids = cmd.ExecuteToList<int>();

                    for ( var i = 0; i < ids.Count; i++ )
                    {
                        subscription.Topics[ i ].SubscriptionTopicId = ids[ i ];
                    }

                    return subscription;
                }

                /// <summary>
                /// Method for acquiring the active mutex.
                /// </summary>
                /// <param name="mutexKey">The mutex key trying to be retrieved.</param>
                /// <returns><see cref="Models.Mutex"/></returns>
                public Models.Mutex GetActiveMutex( string mutexKey )
                {
                    var hash = GenerateHash( mutexKey );

                    var cmd = GetDbCommand()
                        .SetCommandText( @"
SELECT TOP 1
    em.EventMutexId
FROM
    [dbo].[EventMutex] em
    LEFT JOIN [dbo].[EventMutexReleased] emr ON emr.EventMutexId = em.EventMutexId
WHERE
    Hash = @hash
    AND Expiration > GETDATE()
    AND emr.EventMutexReleasedId IS NULL
" ).AddParameter( "@hash", hash, DbType.Binary );

                    var id = cmd.ExecuteScalar<long?>();

                    return id != null ? new Models.Mutex { MutexId = id } : null;
                }

                /// <summary>
                /// Method for trying to acquire a mutex.
                /// </summary>
                /// <param name="mutexKey">The mutex key trying to be acquired.</param>
                /// <param name="timeToLive">The TimeSpan that the mutex should live.</param>
                /// <returns><see cref="Models.Mutex"/></returns>
                public Models.Mutex TryAcquireMutex( string mutexKey, TimeSpan timeToLive )
                {
                    var hash = GenerateHash( mutexKey );

                    var cmd = GetDbCommand()
                        .SetCommandText( @"
-- acquire lock
DECLARE @result INT
EXEC @result = sp_getapplock @Resource = @key, -- name of the mutex
    @LockMode = 'Exclusive', --This session is the only one who can have this lock.
    @LockOwner = 'Session', --lock lives while this session is active.
    @LockTimeout = @timeout,  -- anyone else trying to acquire this lock will wait this long before the thread returns a negative result. 
    @DbPrincipal = 'public'

-- lock acquired
IF @result IN ( 0, 1 )
    BEGIN
        IF NOT EXISTS ( SELECT TOP 1
                            1
                        FROM
                            [dbo].[EventMutex] em
                            LEFT JOIN [dbo].[EventMutexReleased] emr ON emr.EventMutexId = em.EventMutexId
                        WHERE
                            Hash = @hash
                            AND Expiration > GETDATE()
                            AND emr.EventMutexReleasedId IS NULL )
            BEGIN 
                INSERT  INTO [dbo].[EventMutex]
                        ( [Key]
                        ,[Hash]
                        ,[Expiration]
                        ,[CreatedByUser]
                        ,[CreatedByApplication]
                        )
                VALUES
                        ( @key
                        ,@hash
                        ,@expiration
                        ,@user
                        ,@app
                        )

                SELECT SCOPE_IDENTITY();
            END 

        EXEC sp_releaseapplock @Resource = @key, @LockOwner = 'Session'
    END" )
                        .AddParameter( "@key", mutexKey, DbType.AnsiString )
                        .AddParameter( "@timeout", Configuration.MutexAcquisitionWaitTime, DbType.Int32 )
                        .AddParameter( "@hash", hash, DbType.Binary )
                        .AddParameter( "@expiration", DateTime.Now.Add( timeToLive ), DbType.DateTime )
                        .AddParameter( "@user", Configuration.UserName, DbType.AnsiString )
                        .AddParameter( "@app", Configuration.AppName, DbType.AnsiString );

                    var result = cmd.ExecuteScalar<long?>();

                    return result != null
                        ? new Models.Mutex { MutexId = result }
                        : null;
                }

                /// <summary>
                /// Method for releasing a mutex.
                /// </summary>
                /// <param name="mutex">The <see cref="Models.Mutex"/> you are trying to release.</param>
                public void ReleaseMutex( Models.Mutex mutex )
                {
                    if ( mutex == null )
                        return;

                    var cmd = GetDbCommand()
                        .GenerateInsertForSqlServer(
                            new
                            {
                                EventMutexId = mutex.MutexId,
                                CreatedByUser = Configuration.UserName,
                                CreatedByApplication = Configuration.AppName
                            }, "EventMutexReleased" );

                    cmd.ExecuteNonQuery();
                }

                /// <summary>
                /// Method for converting the db result to <see cref="Models.Event"/>
                /// </summary>
                /// <param name="dbResult">The result of the database query.</param>
                /// <returns><see cref="Models.Event"/></returns>
                private static Models.Event ToEvent( List<dynamic> dbResult )
                {
                    if ( !dbResult.Any() ) return null;

                    var eventResult = dbResult.First();

                    var @event = new Models.Event
                    {
                        EventId = eventResult.EventId,
                        EventPayload = Configuration.JsonSerializer.Deserialize<object>( eventResult.EventData ),
                        Topics = Configuration.JsonSerializer.Deserialize<Dictionary<string, string>>( eventResult.TopicData ),
                        TopicCount = eventResult.TopicCount,
                        CreateDate = eventResult.CreateDate,
                        CreatedByApplication = eventResult.CreatedByApplication,
                        CreatedByUser = eventResult.CreatedByUser,
                        CreatedByHostName = eventResult.CreatedByHostName
                    };

                    foreach ( var topic in dbResult.Where( x => x.EventTopicId != null ) )
                    {
                        @event.EventTopics.Add( new Models.EventTopic
                        {
                            EventTopicId = topic.EventTopicId,
                            EventId = @event.EventId,
                            Key = topic.Key,
                            Value = topic.Value,
                            CreatedByHostName = topic.TopicCreatedByHostName,
                            CreateDate = topic.TopicCreateDate,
                            CreatedByApplication = topic.TopicCreatedByApplication,
                            CreatedByUser = topic.TopicCreatedByUser
                        } );
                    }

                    return @event;
                }

                /// <summary>
                /// Comparision functions for subscriptions.
                /// </summary>
                /// <typeparam name="T"></typeparam>
                private class SubscriptionTopicComparer<T> : IEqualityComparer<T>
                {
                    public bool Equals( T s1, T s2 )
                    {
                        dynamic d1 = s1;
                        dynamic d2 = s2;

                        return d1.SubscriptionId == d2.SubscriptionId &&
                               d1.Key == d2.Key;
                    }

                    public int GetHashCode( T s )
                    {
                        dynamic d = s;

                        return d.SubscriptionId.GetHashCode() ^ d.Key.GetHashCode();
                    }
                }

                /// <summary>
                /// Method wrapping the sequelocity get database command for sql server.
                /// </summary>
                /// <returns><see cref="DatabaseCommand"/></returns>
                private static DatabaseCommand GetDbCommand()
                {
                    if ( Configuration.DatabaseConnectionString == null )
                        throw new Exception( "No function provided for retrieving Database connection string" );

                    var connectionString = Configuration.DatabaseConnectionString();

                    var builder = new SqlConnectionStringBuilder( connectionString )
                    {
                        ApplicationName = Configuration.AppName
                    };

                    return SequelocityDotNet.Sequelocity.GetDatabaseCommandForSqlServer( builder.ToString() );
                }

                /// <summary>
                /// Method for generating SHA1 hash. This is used for mutex keys.
                /// </summary>
                /// <param name="value">Value to be hashed.</param>
                /// <returns><see cref="T:byte[]"/></returns>
                private static byte[] GenerateHash( string value )
                {
                    byte[] hash;
                    using ( var provider = new SHA1CryptoServiceProvider() )
                    {
                        hash = provider.ComputeHash( Encoding.ASCII.GetBytes( value ) );
                    }

                    return hash;
                }
            }
        }

        /// <summary>
        /// Class for storing constants and basic utils.
        /// </summary>
        public static class Utils
        {

            /// <summary>
            /// Event Activity Type Code Constants.
            /// </summary>
            public static class EventSubscriptionActivityTypeCode
            {
                public const string EventProcessingMutexCouldNotBeAcquired = "EVENT_PROCESS_REQUEST_ERROR_MUTEX_COULD_NOT_BE_ACQUIRED";
                public const string SubscriptionsFound = "SUBSCRIPTIONS_FOUND";
                public const string SubscriptionTransformFunctionInvalid = "SUBSCRIPTION_TRANSFORM_FUNCTION_INVALID";
                public const string EventProcessingError = "EVENT_PROCESSING_ERROR";
                public const string SubscriptionRequestSent = "SUBSCRIPTION_REQUEST_SENT";
                public const string SubscriptionRequestError = "SUBSCRIPTION_REQUEST_ERROR";
                public const string SubscriptionRequestErrorMutexCouldNotBeAcquired = "SUBSCRIPTION_REQUEST_ERROR_MUTEX_COULD_NOT_BE_ACQUIRED";
                public const string SubscriptionResponseOk = "SUBSCRIPTION_RESPONSE_OK";
                public const string SubscriptionResponseError = "SUBSCRIPTION_RESPONSE_ERROR";
                public const string EventSubscriptionPreviouslyFulfilled = "EVENT_SUBSCRIPTION_PREVIOUSLY_FULFILLED";
                public const string InvokeServiceRequestGenerated = "INVOKE_SERVICE_REQUEST_GENERATED";
            }

            /// <summary>
            /// Operator Constatnts.
            /// </summary>
            public static class Operator
            {
                public const string Equal = "EQUALS";
                public const string Like = "LIKE";
                public const string In = "IN";
                public const string NotIn = "NOT_IN";
            }

            /// <summary>
            /// Json Serializer.
            /// </summary>
            public class JsonSerializer
            {
                public string Serialize( object obj )
                {
                    return JsonConvert.SerializeObject( obj );
                }

                public T Deserialize<T>( string json )
                {
                    return JsonConvert.DeserializeObject<T>( json );
                }
            }
        }

        /// <summary>
        /// Spoke Configuration. This is all of the possible settings that can be modified throughout spoke.
        /// </summary>
        public class SpokeConfiguration
        {
            public int? DefaultAbortAfterMinutes = 60;
            public int? LiveRetryAbortAfterMinutes = 60;
            public int? EventProcessingMutexTimeToLiveMinutes = 2;
            public int? EventSubscriptionMutexTimeToLiveMinutes = 2;
            public int? FailedEventsLookbackMinutes = 1440;
            public int? FailedEventsThresholdMinutes = 60;
            public int? FailedNotificationsThresholdMinutes = 60;
            public int? MutexAcquisitionWaitTime = 1;
            public int? ClockBackfillTotalMinutes = 10;
            public int? ClockBackfillOffsetMinutes = 2;
            public int? ClockBackfillMutexTimeToLiveMinutes = 2;
            public int? ClockSleepMilliseconds = 10000;
            public bool? SendClockMessages = true;
            public string AppName = $"{Environment.MachineName}-{AppDomain.CurrentDomain.FriendlyName}";
            public string UserName = WindowsIdentity.GetCurrent()?.Name;
            public Func<Models.GetApiUriInput, string> GetApiUri = x => x.Uri;
            public Dictionary<string, Func<Models.WasApiCallSuccessfulInput, bool>> WasApiCallSuccessfulHandlers =
                new Dictionary<string, Func<Models.WasApiCallSuccessfulInput, bool>>
                {
                    { "DEFAULT",
                    response => response.HttpResponse.Exception == null
                                && response.HttpResponse.Response != null
                                && response.HttpResponse.Response.StatusCode == HttpStatusCode.OK}
                };
            public Utils.JsonSerializer JsonSerializer = new Utils.JsonSerializer();
            public Func<DatabaseIO.ISpokeDatabase> Database = () => new DatabaseIO.SpokeSqlDatabase();
            public Func<string> DatabaseConnectionString = () => ConfigurationManager.ConnectionStrings[ "spoke" ].ConnectionString;
        }
    }

    /// <summary>
    /// Extensions used throughout Spoke.
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Extension for turning a dictionary into a name value collection.
        /// </summary>
        /// <param name="source">The source dictionary</param>
        /// <returns><see cref="NameValueCollection"/></returns>
        public static NameValueCollection ToNameValueCollection( this Dictionary<string, string> source )
        {
            NameValueCollection retVal = new NameValueCollection();

            foreach ( var obj in source )
            {
                retVal.Add( obj.Key, obj.Value );
            }

            return retVal;
        }

        /// <summary>
        /// Extension for updating auditing information.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="auditEntity"></param>
        /// <returns></returns>
        public static T Stamp<T>( this T auditEntity ) where T : Spoke.Models.Audit
        {
            auditEntity.CreateDate = DateTime.Now;
            auditEntity.CreatedByApplication = Spoke.Configuration.AppName;
            auditEntity.CreatedByUser = Spoke.Configuration.UserName;

            return auditEntity;
        }

        /// <summary>
        /// Extension for converting object o <see cref="int"/>
        /// </summary>
        /// <param name="value"></param>
        /// <returns><see cref="int"/></returns>
        public static int ToInt( this object value )
        {
            return Convert.ToInt32( value );
        }

        /// <summary>
        /// Extension for normalizing topic keys.
        /// </summary>
        /// <param name="dict">Event Topics</param>
        /// <returns>Dictionary of normalized event topics</returns>
        public static IDictionary<string, string> NormalizeKeys( this IEnumerable<KeyValuePair<string, string>> dict )
        {
            return dict.ToDictionary( kvp => NormalizeKey( kvp.Key ), kvp => kvp.Value );
        }

        /// <summary>
        /// Extension for normalizing subscription topic keys.
        /// </summary>
        /// <param name="topics">IEnumnerable of <see cref="Spoke.Models.SubscriptionTopic"/></param>
        /// <returns>IEnumnerable of <see cref="Spoke.Models.SubscriptionTopic"/></returns>
        public static IEnumerable<Spoke.Models.SubscriptionTopic> NormalizeKeys( this IEnumerable<Spoke.Models.SubscriptionTopic> topics )
        {
            return topics.Select( topic =>
                new Spoke.Models.SubscriptionTopic
                {
                    Key = NormalizeKey( topic.Key ),
                    Value = topic.Value,
                    OperatorTypeCode = topic.OperatorTypeCode
                } ).ToList();
        }

        /// <summary>
        /// Extension to normalize a single string.
        /// </summary>
        /// <param name="input">Topic Key.</param>
        /// <returns><see cref="string"/></returns>
        public static string NormalizeKey( this string input )
        {
            if ( string.IsNullOrEmpty( input ) )
                return input;

            var regex = new Regex( "([a-z])([A-Z])" );

            return regex.Replace( input, "$1_$2" ).ToUpper();
        }

        /// <summary>
        /// Extension for validating Topic Keys.
        /// </summary>
        /// <param name="keys">Topics</param>
        /// <returns><see cref="bool"/></returns>
        public static bool ValidateTopicKeys( this IDictionary<string, string> keys )
        {
            var regex = new Regex( "^[a-zA-Z0-9_]+$" );

            return keys.All( x => regex.IsMatch( x.Key ) );
        }

        /// <summary>
        /// Extension for validating topic values
        /// </summary>
        /// <param name="values">Topics</param>
        /// <returns><see cref="bool"/></returns>
        public static bool ValidateTopicValues( this IDictionary<string, string> values )
        {
            return values.All( x => x.Value != null );
        }

        /// <summary>
        /// Extension for setting event subscription id's on event subscriptions
        /// </summary>
        /// <param name="notifications">List of <see cref="Spoke.Models.SubscriptionNotification"/></param>
        /// <param name="eventSubscriptions">IEnumberable of <see cref="Spoke.Models.EventSubscription"/></param>
        /// <returns>List of <see cref="Spoke.Models.SubscriptionNotification"/></returns>
        public static List<Spoke.Models.SubscriptionNotification> SetEventSubscriptionIds( this List<Spoke.Models.SubscriptionNotification> notifications, IEnumerable<Spoke.Models.EventSubscription> eventSubscriptions )
        {
            foreach ( var eventSubscription in eventSubscriptions )
            {
                var notification =
                    notifications.FirstOrDefault(
                        x => x.EventSubscription.SubscriptionId.ToInt() == eventSubscription.SubscriptionId.ToInt() );

                if ( notification != null )
                    notification.EventSubscription.EventSubscriptionId = eventSubscription.EventSubscriptionId;
            }

            return notifications;
        }
    }

    #region Open Source Attributions

    /*
    Open Source Attributions
    ------------------------
    Spoke has made use of or references the following open source software:

    - JSON.NET: https://github.com/JamesNK/Newtonsoft.Json
            
            The MIT License
            Copyright (c) 2007 James Newton-King
            License available at: https://github.com/JamesNK/Newtonsoft.Json/blob/master/LICENSE.md

    - JINT: https://github.com/sebastienros/jint

            BSD 2-Clause License
            Copyright (c) 2013, Sebastien Ros
            License available at: https://github.com/sebastienros/jint/blob/master/LICENSE.txt

    - Sequelocity.NET: https://github.com/AmbitEnergyLabs/Sequelocity.NET

            The MIT License
            Copyright (c) 2015 Ambit Energy
            License available at: https://github.com/AmbitEnergyLabs/Sequelocity.NET/blob/master/LICENSE
    */
    #endregion
}
