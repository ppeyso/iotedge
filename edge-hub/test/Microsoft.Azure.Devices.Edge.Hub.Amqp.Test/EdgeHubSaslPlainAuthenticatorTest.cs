// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Azure.Devices.Edge.Hub.Amqp.Test
{
    using System;
    using System.Security.Principal;
    using Microsoft.Azure.Devices.Edge.Hub.Core;
    using Microsoft.Azure.Devices.Edge.Hub.Core.Identity;
    using Microsoft.Azure.Devices.Edge.Util.Test.Common;
    using Moq;
    using Xunit;

    public class EdgeHubSaslPlainAuthenticatorTest
    {
        [Fact]
        [Unit]
        public void TestNullConstructorInputs()
        {
            var authenticator = Mock.Of<IAuthenticator>();
            var identityFactory = Mock.Of<IClientCredentialsFactory>();

            Assert.Throws<ArgumentNullException>(() => new EdgeHubSaslPlainAuthenticator(null, identityFactory, "iothub"));
            Assert.Throws<ArgumentNullException>(() => new EdgeHubSaslPlainAuthenticator(authenticator, null, "iothub"));
            Assert.Throws<ArgumentException>(() => new EdgeHubSaslPlainAuthenticator(authenticator, identityFactory, null));
            Assert.NotNull(new EdgeHubSaslPlainAuthenticator(authenticator, identityFactory, "iothub"));
        }

        [Fact]
        [Unit]
        public async void TestBadInputsToAuthenticateAsync()
        {
            var authenticator = Mock.Of<IAuthenticator>();
            var identityFactory = Mock.Of<IClientCredentialsFactory>();
            var saslAuthenticator = new EdgeHubSaslPlainAuthenticator(authenticator, identityFactory, "iothub");

            await Assert.ThrowsAsync<ArgumentException>(() => saslAuthenticator.AuthenticateAsync(null, "pwd"));
            await Assert.ThrowsAsync<ArgumentException>(() => saslAuthenticator.AuthenticateAsync("uid", null));
        }

        [Fact]
        [Unit]
        public async void TestNoDeviceId()
        {
            var authenticator = Mock.Of<IAuthenticator>();
            var identityFactory = Mock.Of<IClientCredentialsFactory>();
            var saslAuthenticator = new EdgeHubSaslPlainAuthenticator(authenticator, identityFactory, "iothub");
            const string UserId = "key1@sas.root.hub1";

            await Assert.ThrowsAsync<EdgeHubConnectionException>(() => saslAuthenticator.AuthenticateAsync(UserId, "pwd"));
        }

        [Fact]
        [Unit]
        public async void TestGetSasTokenFailed()
        {
            var authenticator = Mock.Of<IAuthenticator>();
            var clientCredentialsFactory = Mock.Of<IClientCredentialsFactory>();
            var saslAuthenticator = new EdgeHubSaslPlainAuthenticator(authenticator, clientCredentialsFactory, "iothub");
            const string UserId = "dev1/modules/mod1@sas.hub1";
            const string Password = "pwd";

            Mock.Get(clientCredentialsFactory).Setup(f => f.GetWithSasToken("dev1", "mod1", string.Empty, Password, false))
                .Throws(new ApplicationException("Bad donut"));

            await Assert.ThrowsAsync<EdgeHubConnectionException>(() => saslAuthenticator.AuthenticateAsync(UserId, Password));
        }

        [Fact]
        [Unit]
        public async void TestAuthFailed()
        {
            var authenticator = Mock.Of<IAuthenticator>();
            var clientCredentialsFactory = Mock.Of<IClientCredentialsFactory>();
            var saslAuthenticator = new EdgeHubSaslPlainAuthenticator(authenticator, clientCredentialsFactory, "iothub");
            var clientCredentials = Mock.Of<IClientCredentials>();
            const string UserId = "dev1/modules/mod1@sas.hub1";
            const string Password = "pwd";

            Mock.Get(clientCredentialsFactory).Setup(f => f.GetWithSasToken("dev1", "mod1", string.Empty, Password, false))
                .Returns(clientCredentials);
            Mock.Get(authenticator).Setup(a => a.AuthenticateAsync(clientCredentials))
                .ReturnsAsync(false);

            await Assert.ThrowsAsync<EdgeHubConnectionException>(() => saslAuthenticator.AuthenticateAsync(UserId, Password));
        }

        [Fact]
        [Unit]
        public async void TestAuthSucceeds()
        {
            var authenticator = Mock.Of<IAuthenticator>();
            var clientCredentialsFactory = Mock.Of<IClientCredentialsFactory>();
            var saslAuthenticator = new EdgeHubSaslPlainAuthenticator(authenticator, clientCredentialsFactory, "hub1");
            var identity = new ModuleIdentity("hub1", "dev1", "mod1");
            var clientCredentials = Mock.Of<IClientCredentials>(c => c.Identity == identity);
            const string UserId = "dev1/modules/mod1@sas.hub1";
            const string Password = "pwd";

            Mock.Get(clientCredentialsFactory).Setup(f => f.GetWithSasToken("dev1", "mod1", string.Empty, Password, false))
                .Returns(clientCredentials);
            Mock.Get(authenticator).Setup(a => a.AuthenticateAsync(clientCredentials))
                .ReturnsAsync(true);

            IPrincipal principal = await saslAuthenticator.AuthenticateAsync(UserId, Password);
            Assert.NotNull(principal);

            var amqpAuthenticator = principal as IAmqpAuthenticator;
            Assert.NotNull(amqpAuthenticator);

            bool isAuthenticated = await amqpAuthenticator.AuthenticateAsync("dev1/mod1");
            Assert.True(isAuthenticated);
        }
    }
}
