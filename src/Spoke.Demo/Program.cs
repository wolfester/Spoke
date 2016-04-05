using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Net;
using System.Security.Principal;
using System.Text;
using System.Threading;
using Nano.Web.Core;
using Nano.Web.Core.Host.HttpListener;
using Topshelf;

namespace Spoke.Demo
{
    internal class Program
    {
        private static void Main( string[] args )
        {
            string url = "http://localhost:4546";

            HostFactory.Run( x =>
            {
                x.AddCommandLineDefinition( "uri", input => { url = input; } );
                x.ApplyCommandLine();

                x.Service< StartNano >( settings =>
                {
                    settings.ConstructUsing(
                        hostSettings => new StartNano( hostSettings.ServiceName ) );
                    settings.WhenStarted( nano => nano.Start( url ) );
                    settings.WhenStopped( nano => nano.Stop() );
                } );

                x.StartAutomatically();
            } );
        }

        public class StartNano
        {
            private HttpListenerNanoServer _server;

            private readonly string _applicationName;

            public StartNano( string applicationName )
            {
                _applicationName = applicationName;
            }

            public void Start( string url )
            {
                #region NanoSetup

                var exitEvent = new ManualResetEvent( false );

                Console.CancelKeyPress += ( sender, eventArgs ) =>
                {
                    eventArgs.Cancel = true;
                    exitEvent.Set();
                };

                var validatedUrls = ValidateUrls( url );

                var config = new NanoConfiguration();

                config.EnableCorrelationId();

                config.AddDirectory( "/", Debugger.IsAttached ? "../../www" : "www",
                    returnHttp404WhenFileWasNotFound: true );

                #endregion

                //you will need to provide a connection string in your app.config for this to produce any events.
                //constructor will set default hardcoded values, you can then override any of those values here ( below values listed are the default values )
                var spokeConfig = new Spoke.SpokeConfiguration
                {
                    DefaultAbortAfterMinutes = 60,
                    LiveRetryAbortAfterMinutes = 60,
                    EventProcessingMutexTimeToLiveMinutes = 2,
                    EventSubscriptionMutexTimeToLiveMinutes = 2,
                    FailedEventsLookbackMinutes = 1440,
                    FailedEventsThresholdMinutes = 60,
                    FailedNotificationsThresholdMinutes = 60,
                    MutexAcquisitionWaitTime = 1,
                    ClockBackfillTotalMinutes = 10,
                    ClockBackfillOffsetMinutes = 2,
                    ClockBackfillMutexTimeToLiveMinutes = 2,
                    ClockSleepMilliseconds = 10000,
                    SendClockMessages = true,
                    AppName = $"{Environment.MachineName}-{AppDomain.CurrentDomain.FriendlyName}",
                    UserName = WindowsIdentity.GetCurrent()?.Name,
                    GetApiUri = x => x.Uri,
                    WasApiCallSuccessfulHandlers =
                        new Dictionary< string, Func< Spoke.Models.WasApiCallSuccessfulInput, bool > >
                        {
                            {
                                "DEFAULT",
                                response => response.HttpResponse.Exception == null
                                            && response.HttpResponse.Response != null
                                            && response.HttpResponse.Response.StatusCode == HttpStatusCode.OK
                            }
                        },
                    JsonSerializer = new Spoke.Utils.JsonSerializer(),
                    Database = () => new Spoke.DatabaseIO.SpokeSqlDatabase(),
                    DatabaseConnectionString = () => ConfigurationManager.ConnectionStrings[ "spokePostgres" ].ConnectionString
                };

                //you can add support for "service types" by adding WasApiCallSuccessfulHandlers instead of the default.
                spokeConfig.WasApiCallSuccessfulHandlers.Add( 
                    "OTHER_SERVICE_TYPE", //the key for the new service type
                    response => response.HttpResponse.Exception == null //func to evaluate if the service call was successful or not.
                    );

                var spoke = new Spoke( spokeConfig );
                spoke.Start();

                config.AddMethods<Spoke.ExternalApi>( "/Api/Events" );
                config.AddMethods<Spoke.InternalApi>( "/Api/Events/Internal" );

                #region NanoSetup
                config.AddBackgroundTask( "GCCollect", 30000, () =>
                {
                    GC.Collect();
                    return null;
                } );

                config.GlobalEventHandler.UnhandledExceptionHandlers.Add( ( exception, context ) =>
                {
                    try
                    {
                        if ( !EventLog.SourceExists( _applicationName ) )
                            EventLog.CreateEventSource( _applicationName, "Application" );

                        var msg = new StringBuilder()
                            .AppendLine( "Nano Error:" )
                            .AppendLine( "-----------" ).AppendLine()
                            .AppendLine( "URL: " + context.Request.Url ).AppendLine()
                            .AppendLine( "Message: " + exception.Message ).AppendLine()
                            .AppendLine( "StackTrace:" )
                            .AppendLine( exception.StackTrace )
                            .ToString();

                        EventLog.WriteEntry( _applicationName, msg, EventLogEntryType.Error );
                    }
                    catch ( Exception )
                    {
                        // Gulp: Never throw an exception in the unhandled exception handler
                    }
                } );

                _server = HttpListenerNanoServer.Start( config, validatedUrls );

                if ( Debugger.IsAttached )
                    Process.Start( _server.HttpListenerConfiguration.GetFirstUrlBeingListenedOn().TrimEnd( '/' ) +
                                   "/ApiExplorer/" );

                Console.WriteLine( "Nano Server is running on: " +
                                   _server.HttpListenerConfiguration.GetFirstUrlBeingListenedOn() );
                #endregion
            }

            #region NanoSetup
            public void Stop()
            {
                if ( _server.HttpListenerConfiguration.HttpListener.IsListening )
                {
                    _server.HttpListenerConfiguration.HttpListener.Stop();
                }
            }

            private string[] ValidateUrls( string urls )
            {
                if ( string.IsNullOrWhiteSpace( urls ) )
                    throw new Exception( "No URIs were supplied" );

                string[] urlArray = urls.Split( ',' );

                var list = new List<string>();

                foreach ( var url in urlArray )
                {
                    var correctedUrl = url.Replace( "\\", "" ).Replace( "\"", "" );

                    try
                    {
                        var uri = new Uri( correctedUrl ); // Validate that the URL is valid
                        list.Add( uri.ToString() );
                    }
                    catch ( Exception ex )
                    {
                        throw new Exception( "The following URI is not valid: " + correctedUrl + " Exception: " +
                                             ex.Message );
                    }
                }

                return list.ToArray();
            } 
            #endregion
        }
    }
}
