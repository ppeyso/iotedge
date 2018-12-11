// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Azure.Devices.Edge.Agent.Edgelet.Version_2018_12_30
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Edge.Agent.Core;
    using Microsoft.Azure.Devices.Edge.Agent.Edgelet.Version_2018_12_30.GeneratedCode;
    using Microsoft.Azure.Devices.Edge.Agent.Edgelet.Versioning;
    using Microsoft.Azure.Devices.Edge.Util;
    using Microsoft.Azure.Devices.Edge.Util.TransientFaultHandling;
    using Newtonsoft.Json.Linq;
    using ApiVersion = Util.Edged.ApiVersion;
    using ModuleSpec = Models.ModuleSpec;
    using SystemInfo = GeneratedCode.SystemInfo;

    class ModuleManagementHttpClient : ModuleManagementHttpClientVersioned
    {
        public ModuleManagementHttpClient(Uri managementUri) : base(managementUri, ApiVersion.Version20181230)
        { }

        public override async Task<Models.Identity> CreateIdentityAsync(string name, string managedBy)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                Identity identity = await this.Execute(
                    () => edgeletHttpClient.CreateIdentityAsync(this.Version.Name, new IdentitySpec
                    {
                        ModuleId = name,
                        ManagedBy = managedBy
                    }),
                    $"Create identity for {name}");
                return this.MapFromIdentity(identity);
            }
        }

        public override async Task<Models.Identity> UpdateIdentityAsync(string name, string generationId, string managedBy)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(base.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                Identity identity = await this.Execute(() => edgeletHttpClient.UpdateIdentityAsync(
                    this.Version.Name,
                    name,
                    new UpdateIdentity
                    {
                        GenerationId = generationId,
                        ManagedBy = managedBy
                    }),
                    $"Update identity for {name} with generation ID {generationId}");
                return this.MapFromIdentity(identity);
            }
        }

        public override async Task DeleteIdentityAsync(string name)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.DeleteIdentityAsync(this.Version.Name, name), $"Delete identity for {name}");
            }
        }

        public override async Task<IEnumerable<Models.Identity>> GetIdentities()
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                IdentityList identityList = await this.Execute(() => edgeletHttpClient.ListIdentitiesAsync(this.Version.Name), $"List identities");
                return identityList.Identities.Select(i => this.MapFromIdentity(i));
            }
        }

        public override async Task CreateModuleAsync(ModuleSpec moduleSpec)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.CreateModuleAsync(this.Version.Name, this.MapToModuleSpec(moduleSpec)), $"Create module {moduleSpec.Name}");
            }
        }

        public override async Task DeleteModuleAsync(string name)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.DeleteModuleAsync(this.Version.Name, name), $"Delete module {name}");
            }
        }

        public override async Task RestartModuleAsync(string name)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(
                    () => edgeletHttpClient.RestartModuleAsync(this.Version.Name, name),
                    $"Restart module {name}");
            }
        }

        public override async Task<Core.SystemInfo> GetSystemInfoAsync()
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                SystemInfo systemInfo = await this.Execute(
                    () => edgeletHttpClient.GetSystemInfoAsync(this.Version.Name),
                    "Getting System Info");
                return new Core.SystemInfo(systemInfo.OsType, systemInfo.Architecture, systemInfo.Version);
            }
        }

        public override async Task<IEnumerable<ModuleRuntimeInfo>> GetModules<T>(CancellationToken cancellationToken)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                ModuleList moduleList = await this.Execute(
                    () => edgeletHttpClient.ListModulesAsync(this.Version.Name, cancellationToken),
                    $"List modules");
                return moduleList.Modules.Select(m => this.GetModuleRuntimeInfo<T>(m));
            }
        }

        public override async Task StartModuleAsync(string name)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.StartModuleAsync(this.Version.Name, name), $"start module {name}");
            }
        }

        public override async Task StopModuleAsync(string name)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.StopModuleAsync(this.Version.Name, name), $"stop module {name}");
            }
        }

        public override async Task UpdateModuleAsync(ModuleSpec moduleSpec)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.UpdateModuleAsync(this.Version.Name, moduleSpec.Name, null, this.MapToModuleSpec(moduleSpec)), $"update module {moduleSpec.Name}");
            }
        }

        public override async Task UpdateAndStartModuleAsync(ModuleSpec moduleSpec)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.UpdateModuleAsync(this.Version.Name, moduleSpec.Name, true, this.MapToModuleSpec(moduleSpec)), $"update and start module {moduleSpec.Name}");
            }
        }

        public override async Task PrepareUpdateAsync(ModuleSpec moduleSpec)
        {
            using (HttpClient httpClient = HttpClientHelper.GetHttpClient(this.ManagementUri))
            {
                var edgeletHttpClient = new EdgeletHttpClient(httpClient) { BaseUrl = HttpClientHelper.GetBaseUrl(this.ManagementUri) };
                await this.Execute(() => edgeletHttpClient.PrepareUpdateModuleAsync(this.Version.Name, moduleSpec.Name, this.MapToModuleSpec(moduleSpec)), $"prepare update for module module {moduleSpec.Name}");
            }
        }

        protected override void HandleException(Exception exception, string operation)
        {
            switch (exception)
            {

                case SwaggerException<ErrorResponse> errorResponseException:
                    throw new EdgeletCommunicationException($"Error calling {operation}: {errorResponseException.Result?.Message ?? string.Empty}", errorResponseException.StatusCode);

                case SwaggerException swaggerException:
                    if (swaggerException.StatusCode < 400)
                    {
                        return;
                    }
                    else
                    {
                        throw new EdgeletCommunicationException($"Error calling {operation}: {swaggerException.Response ?? string.Empty}", swaggerException.StatusCode);
                    }
                default:
                    throw exception;
            }
        }

        protected override ITransientErrorDetectionStrategy GetTransientErrorDetectionStartegy() => new ErrorDetectionStrategy();

        GeneratedCode.ModuleSpec MapToModuleSpec(ModuleSpec moduleSpec)
        {
            return new GeneratedCode.ModuleSpec()
            {
                Name = moduleSpec.Name,
                Type = moduleSpec.Type,
                Config = new Config()
                {
                    Env = new ObservableCollection<EnvVar>(moduleSpec.EnvironmentVariables.Select(e => new EnvVar()
                    {
                        Key = e.Key,
                        Value = e.Value
                    }).ToList()),
                    Settings = moduleSpec.Settings
                }
            };
        }

        Models.Identity MapFromIdentity(Identity identity)
        {
            return new Models.Identity(identity.ModuleId, identity.GenerationId, identity.ManagedBy);
        }

        ModuleRuntimeInfo<T> GetModuleRuntimeInfo<T>(ModuleDetails moduleDetails)
        {
            ExitStatus exitStatus = moduleDetails.Status.ExitStatus;
            if (exitStatus == null || !long.TryParse(exitStatus.StatusCode, out long exitCode))
            {
                exitCode = 0;
            }

            Option<DateTime> exitTime = exitStatus == null ? Option.None<DateTime>() : Option.Some(exitStatus.ExitTime);
            Option<DateTime> startTime = !moduleDetails.Status.StartTime.HasValue ? Option.None<DateTime>() : Option.Some(moduleDetails.Status.StartTime.Value);

            if (!Enum.TryParse(moduleDetails.Status.RuntimeStatus.Status, true, out ModuleStatus status))
            {
                status = ModuleStatus.Unknown;
            }

            if (!(moduleDetails.Config.Settings is JObject jobject))
            {
                throw new InvalidOperationException($"Module config is of type {moduleDetails.Config.Settings.GetType()}. Expected type JObject");
            }
            var config = jobject.ToObject<T>();

            var moduleRuntimeInfo = new ModuleRuntimeInfo<T>(moduleDetails.Name, moduleDetails.Type, status,
                moduleDetails.Status.RuntimeStatus.Description, exitCode, startTime, exitTime, config);
            return moduleRuntimeInfo;
        }

        class ErrorDetectionStrategy : ITransientErrorDetectionStrategy
        {
            public bool IsTransient(Exception ex) => ex is SwaggerException se
                && se.StatusCode >= 500;
        }
    }
}
