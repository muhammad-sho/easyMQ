import type {
  IAuthenticateGeneric,
  ICredentialTestRequest,
  ICredentialType,
  INodeProperties,
} from "n8n-workflow";

export class EasyMqApi implements ICredentialType {
  name = "easyMqApi";

  displayName = "EasyMQ API";

  documentationUrl = "https://github.com/muhammad-sho/easyMQ/tree/main/n8n-nodes-easymq";

  properties: INodeProperties[] = [
    {
      displayName: "Base URL",
      name: "baseUrl",
      type: "string",
      default: "http://localhost:3000",
      placeholder: "http://easymq:3000",
      description: "Base URL of the easyMQ HTTP API (no trailing slash)",
      required: true,
    },
    {
      displayName: "API Token",
      name: "apiToken",
      type: "string",
      typeOptions: { password: true },
      default: "",
      description:
        "Bearer token printed in the easyMQ startup logs (or your pinned API_TOKEN value)",
      required: true,
    },
  ];

  authenticate: IAuthenticateGeneric = {
    type: "generic",
    properties: {
      headers: {
        Authorization: "=Bearer {{$credentials.apiToken}}",
      },
    },
  };

  test: ICredentialTestRequest = {
    request: {
      baseURL: "={{$credentials.baseUrl}}",
      url: "/queues",
      method: "GET",
    },
  };
}
