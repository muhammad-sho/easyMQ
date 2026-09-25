import type {
  IDataObject,
  IExecuteFunctions,
  NodeParameterValueType,
  IHttpRequestMethods,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
} from "n8n-workflow";
import { NodeConnectionTypes, NodeOperationError } from "n8n-workflow";

/** Narrow any API value to n8n's item-data type without unsafe casts. */
function toDataObject(value: unknown): IDataObject {
  if (typeof value === "object" && value !== null) return value as IDataObject;
  return {};
}

/** Narrow node parameters explicitly (avoids overload-resolution surprises). */
function strParam(value: NodeParameterValueType | object, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function numParam(value: NodeParameterValueType | object, fallback: number): number {
  return typeof value === "number" ? value : fallback;
}

interface EasyMqMessageItem {
  id: string;
  queue: string;
  data: unknown;
  deliveryCount: number;
  redelivered: boolean;
  visibleAt: number;
}

function toMessageItem(queue: string, message: Record<string, unknown>): INodeExecutionData {
  return {
    json: toDataObject({
      id: message["id"],
      queue,
      data: message["data"],
      deliveryCount: message["deliveryCount"],
      redelivered: message["redelivered"],
      visibleAt: message["visibleAt"],
    }),
  };
}

export class EasyMq implements INodeType {
  description: INodeTypeDescription = {
    displayName: "EasyMQ",
    name: "easyMq",
    icon: "file:easymq.svg",
    group: ["transform"],
    version: 1,
    subtitle: '={{$parameter["operation"] + ": " + $parameter["resource"]}}',
    description: "Publish and manage messages in easyMQ queues",
    defaults: {
      name: "EasyMQ",
    },
    inputs: [NodeConnectionTypes.Main],
    outputs: [NodeConnectionTypes.Main],
    credentials: [
      {
        name: "easyMqApi",
        required: true,
      },
    ],
    properties: [
      {
        displayName: "Resource",
        name: "resource",
        type: "options",
        noDataExpression: true,
        options: [
          { name: "Message", value: "message" },
          { name: "Queue", value: "queue" },
        ],
        default: "message",
      },
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
        displayOptions: { show: { resource: ["message"] } },
        options: [
          {
            name: "Publish",
            value: "publish",
            description: "Publish a message to a queue",
            action: "Publish a message",
          },
          {
            name: "Consume",
            value: "consume",
            description: "Consume waiting messages (competing-consumer poll)",
            action: "Consume messages",
          },
          {
            name: "Get",
            value: "get",
            description: "Inspect a single message",
            action: "Get a message",
          },
          {
            name: "Acknowledge",
            value: "ack",
            description: "Acknowledge a consumed message",
            action: "Acknowledge a message",
          },
          {
            name: "Requeue",
            value: "requeue",
            description: "Return a consumed message to the queue",
            action: "Requeue a message",
          },
          {
            name: "Delete",
            value: "delete",
            description: "Delete a waiting (queued) message",
            action: "Delete a message",
          },
          {
            name: "Set TTL",
            value: "setTtl",
            description: "Change/reset a waiting message's TTL",
            action: "Set a message TTL",
          },
        ],
        default: "publish",
      },
      {
        displayName: "Operation",
        name: "operation",
        type: "options",
        noDataExpression: true,
        displayOptions: { show: { resource: ["queue"] } },
        options: [
          {
            name: "Declare",
            value: "declare",
            description: "Create a queue if it does not exist (idempotent)",
            action: "Declare a queue",
          },
          {
            name: "Get Statistics",
            value: "stats",
            description: "Ready/delayed/unacked counts plus consumers",
            action: "Get queue statistics",
          },
          {
            name: "List",
            value: "list",
            description: "List all queues with depths",
            action: "List queues",
          },
          {
            name: "Delete",
            value: "delete",
            description: "Delete a queue and every message in it",
            action: "Delete a queue",
          },
        ],
        default: "declare",
      },
      {
        displayName: "Queue",
        name: "queue",
        type: "string",
        default: "",
        required: true,
        displayOptions: {
          hide: { resource: ["queue"], operation: ["list"] },
        },
        description: "Name of the queue",
      },
      {
        displayName: "Message Data",
        name: "messageData",
        type: "json",
        default: '{\n  "message": "hello"\n}',
        required: true,
        displayOptions: { show: { resource: ["message"], operation: ["publish"] } },
        description: "Arbitrary JSON payload — a message is simply a message",
      },
      {
        displayName: "Message ID",
        name: "messageId",
        type: "string",
        default: "",
        displayOptions: {
          show: { resource: ["message"], operation: ["get", "ack", "requeue", "delete", "setTtl"] },
        },
        description: "Unique id of the message (returned by publish/consume)",
        required: true,
      },
      {
        displayName: "Custom Message ID",
        name: "customMessageId",
        type: "string",
        default: "",
        displayOptions: { show: { resource: ["message"], operation: ["publish"] } },
        description: "Optional id for the message (generated when empty; duplicates conflict)",
      },
      {
        displayName: "TTL (Ms)",
        name: "ttlMs",
        type: "number",
        default: 0,
        displayOptions: { show: { resource: ["message"], operation: ["publish"] } },
        description: "Delay before the message becomes available (0 = immediately)",
      },
      {
        displayName: "New TTL (Ms)",
        name: "ttl",
        type: "number",
        default: 60000,
        required: true,
        displayOptions: { show: { resource: ["message"], operation: ["setTtl"] } },
        description: "New TTL counted from now (0 = immediately available)",
      },
      {
        displayName: "Consumer ID",
        name: "consumerId",
        type: "string",
        default: "",
        displayOptions: {
          show: { resource: ["message"], operation: ["consume", "ack", "requeue"] },
        },
        description:
          "Consumer identity for leases and prefetch (generated when empty; required to ack another call's messages reliably)",
      },
      {
        displayName: "Max Messages",
        name: "count",
        type: "number",
        default: 1,
        displayOptions: { show: { resource: ["message"], operation: ["consume"] } },
        description: "Max messages to return in this call",
      },
      {
        displayName: "Visibility Timeout (Ms)",
        name: "visibilityTimeoutMs",
        type: "number",
        default: 30000,
        displayOptions: { show: { resource: ["message"], operation: ["consume"] } },
        description: "Lease per message: unacked past this timeout the message is redelivered",
      },
      {
        displayName: "Prefetch",
        name: "prefetch",
        type: "number",
        default: 100,
        displayOptions: { show: { resource: ["message"], operation: ["consume"] } },
        description: "Max messages leased to this consumer at once",
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];
    const resource = strParam(this.getNodeParameter("resource", 0));
    const operation = strParam(this.getNodeParameter("operation", 0));

    for (let i = 0; i < items.length; i++) {
      try {
        if (resource === "queue") {
          const queue = strParam(this.getNodeParameter("queue", i, ""));
          if (operation === "list") {
            const response = toDataObject(await request.call(this, "GET", "/queues", undefined, i));
            returnData.push({ json: response });
          } else if (operation === "declare") {
            const response = await request.call(this, "PUT", `/queues/${encode(queue)}`, {}, i);
            returnData.push({ json: toDataObject(response) });
          } else if (operation === "stats") {
            const response = await request.call(
              this,
              "GET",
              `/queues/${encode(queue)}`,
              undefined,
              i,
            );
            returnData.push({ json: toDataObject(response) });
          } else if (operation === "delete") {
            await request.call(this, "DELETE", `/queues/${encode(queue)}`, undefined, i);
            returnData.push({ json: { queue, deleted: true } });
          } else {
            throw new NodeOperationError(this.getNode(), `Unknown queue operation "${operation}"`, {
              itemIndex: i,
            });
          }
          continue;
        }

        const queue = strParam(this.getNodeParameter("queue", i, ""));
        if (operation === "publish") {
          const dataParam: unknown = this.getNodeParameter("messageData", i);
          const data: unknown =
            typeof dataParam === "string" ? (JSON.parse(dataParam) as unknown) : dataParam;
          const customId = strParam(this.getNodeParameter("customMessageId", i, "")).trim();
          const ttlMs = numParam(this.getNodeParameter("ttlMs", i, 0), 0);
          const body: Record<string, unknown> = { data };
          if (customId !== "") body["id"] = customId;
          if (ttlMs > 0) body["ttlMs"] = ttlMs;
          const response = await request.call(
            this,
            "POST",
            `/queues/${encode(queue)}/messages`,
            body,
            i,
          );
          returnData.push({ json: toDataObject(response) });
        } else if (operation === "consume") {
          const consumerId = strParam(this.getNodeParameter("consumerId", i, "")).trim();
          const body: Record<string, unknown> = {
            count: numParam(this.getNodeParameter("count", i, 1), 1),
            visibilityTimeoutMs: numParam(
              this.getNodeParameter("visibilityTimeoutMs", i, 30000),
              30000,
            ),
            prefetch: numParam(this.getNodeParameter("prefetch", i, 100), 100),
          };
          if (consumerId !== "") body["consumerId"] = consumerId;
          const response = toDataObject(
            await request.call(this, "POST", `/queues/${encode(queue)}/consume`, body, i),
          );
          const messages = toMessageList(response["messages"]);
          const owner = typeof response["consumerId"] === "string" ? response["consumerId"] : "";
          for (const message of messages) {
            returnData.push(toMessageItem(queue, message));
          }
          if (messages.length === 0) {
            returnData.push({
              json: { queue, consumerId: owner, messages: [] },
            });
          }
        } else if (operation === "get") {
          const messageId = strParam(this.getNodeParameter("messageId", i));
          const response = await request.call(
            this,
            "GET",
            `/queues/${encode(queue)}/messages/${encode(messageId)}`,
            undefined,
            i,
          );
          returnData.push({ json: toDataObject(response) });
        } else if (operation === "ack" || operation === "requeue") {
          const messageId = strParam(this.getNodeParameter("messageId", i));
          const consumerId = strParam(this.getNodeParameter("consumerId", i, "")).trim();
          const body: Record<string, unknown> = {};
          if (consumerId !== "") body["consumerId"] = consumerId;
          const response = await request.call(
            this,
            "POST",
            `/queues/${encode(queue)}/messages/${encode(messageId)}/${operation}`,
            body,
            i,
          );
          returnData.push({ json: toDataObject(response) });
        } else if (operation === "delete") {
          const messageId = strParam(this.getNodeParameter("messageId", i));
          await request.call(
            this,
            "DELETE",
            `/queues/${encode(queue)}/messages/${encode(messageId)}`,
            undefined,
            i,
          );
          returnData.push({ json: { queue, id: messageId, deleted: true } });
        } else if (operation === "setTtl") {
          const messageId = strParam(this.getNodeParameter("messageId", i));
          const ttl = numParam(this.getNodeParameter("ttl", i), 0);
          const response = await request.call(
            this,
            "PUT",
            `/queues/${encode(queue)}/messages/${encode(messageId)}/ttl`,
            { ttl },
            i,
          );
          returnData.push({ json: toDataObject(response) });
        } else {
          throw new NodeOperationError(this.getNode(), `Unknown message operation "${operation}"`, {
            itemIndex: i,
          });
        }
      } catch (error) {
        if (this.continueOnFail()) {
          returnData.push({
            json: { error: error instanceof Error ? error.message : String(error) },
            pairedItem: { item: i },
          });
          continue;
        }
        throw error;
      }
    }
    return [returnData];
  }
}

function toMessageList(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return [];
  return value.filter(
    (entry): entry is Record<string, unknown> => typeof entry === "object" && entry !== null,
  );
}

function encode(value: string): string {
  return encodeURIComponent(value);
}

async function request(
  this: IExecuteFunctions,
  method: IHttpRequestMethods,
  path: string,
  body: Record<string, unknown> | undefined,
  itemIndex: number,
): Promise<IDataObject> {
  const credentials = (await this.getCredentials("easyMqApi")) as unknown as {
    baseUrl?: string;
  };
  const baseUrl = (credentials.baseUrl ?? "").replace(/\/+$/, "");
  try {
    return toDataObject(
      await this.helpers.requestWithAuthentication.call(this, "easyMqApi", {
        method,
        baseURL: baseUrl,
        url: path,
        ...(body !== undefined ? { body } : {}),
        json: true,
      }),
    );
  } catch (error) {
    throw new NodeOperationError(this.getNode(), describeApiError(error), {
      itemIndex,
    });
  }
}

function describeApiError(error: unknown): string {
  if (typeof error === "object" && error !== null) {
    const record = error as {
      response?: { body?: { error?: { code?: string; message?: string } } };
      message?: unknown;
    };
    const apiError = record.response?.body?.error;
    if (apiError?.code && apiError.message) {
      return `easyMQ ${apiError.code}: ${apiError.message}`;
    }
    if (typeof record.message === "string" && record.message !== "") {
      return `easyMQ request failed: ${record.message}`;
    }
  }
  return "easyMQ request failed.";
}

export type { EasyMqMessageItem };
