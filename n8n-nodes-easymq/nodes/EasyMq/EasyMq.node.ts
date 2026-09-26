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
            name: "Acknowledge",
            value: "ack",
            description:
              "Acknowledge a message delivered by the EasyMQ Trigger (also resolves a waiting 'Specified Later in Workflow' trigger)",
            action: "Acknowledge a message",
          },
          {
            name: "Requeue",
            value: "requeue",
            description: "Reject a message and return it to the queue for redelivery",
            action: "Requeue a message",
          },
          {
            name: "Delete",
            value: "delete",
            description: "Delete a waiting (queued) message so it is never delivered",
            action: "Delete a message",
          },
          {
            name: "Set TTL",
            value: "setTtl",
            description: "Change/reset a waiting message's TTL",
            action: "Set a message TTL",
          },
          {
            name: "Get",
            value: "get",
            description: "Inspect a single message",
            action: "Get a message",
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
        displayName: "Queue",
        name: "triggerQueue",
        type: "string",
        // Picks up the queue straight from an EasyMQ Trigger output item,
        // so Acknowledge-family operations need no manual wiring.
        default: "={{ $json.queue }}",
        required: true,
        displayOptions: {
          show: { resource: ["message"], operation: ["ack", "requeue", "delete", "setTtl", "get"] },
        },
        description: "Name of the queue (taken from the trigger item when connected)",
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
        // Picks up the id straight from an EasyMQ Trigger output item when
        // connected; for Publish it is the upsert key (always explicit).
        default: "={{ $json.messageId }}",
        displayOptions: {
          show: {
            resource: ["message"],
            operation: ["publish", "get", "ack", "requeue", "delete", "setTtl"],
          },
        },
        description:
          "Unique id of the message — the upsert key when publishing (taken from the trigger item when connected)",
        required: true,
      },
      {
        displayName: "Upsert",
        name: "upsert",
        type: "boolean",
        default: false,
        displayOptions: { show: { resource: ["message"], operation: ["publish"] } },
        description:
          "Update the message in place when the ID already exists (new data and TTL), instead of failing with a conflict. Leased messages always conflict.",
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
        // Picks up the consumer straight from an EasyMQ Trigger output item.
        default: "={{ $json.consumerId }}",
        displayOptions: {
          show: { resource: ["message"], operation: ["ack", "requeue"] },
        },
        description:
          "Consumer holding the lease — required to settle another consumer's message reliably (taken from the trigger item when connected)",
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

        if (operation === "publish") {
          const queue = strParam(this.getNodeParameter("queue", i, ""));
          const dataParam: unknown = this.getNodeParameter("messageData", i);
          const data: unknown =
            typeof dataParam === "string" ? (JSON.parse(dataParam) as unknown) : dataParam;
          const messageId = strParam(this.getNodeParameter("messageId", i, "")).trim();
          if (messageId === "") {
            throw new NodeOperationError(this.getNode(), "Message ID is required.", {
              itemIndex: i,
            });
          }
          const ttlMs = numParam(this.getNodeParameter("ttlMs", i, 0), 0);
          const upsert = this.getNodeParameter("upsert", i, false);
          const body: Record<string, unknown> = { id: messageId, data };
          if (ttlMs > 0) body["ttlMs"] = ttlMs;
          if (upsert === true) body["upsert"] = true;
          const response = await request.call(
            this,
            "POST",
            `/queues/${encode(queue)}/messages`,
            body,
            i,
          );
          returnData.push({ json: toDataObject(response) });
          continue;
        }

        // Acknowledge-family operations work on trigger output items: queue,
        // message id, and consumer default to the trigger item fields.
        const queue = strParam(this.getNodeParameter("triggerQueue", i, ""));
        const messageId = strParam(this.getNodeParameter("messageId", i));
        if (operation === "get") {
          const response = await request.call(
            this,
            "GET",
            `/queues/${encode(queue)}/messages/${encode(messageId)}`,
            undefined,
            i,
          );
          returnData.push({ json: toDataObject(response) });
        } else if (operation === "ack" || operation === "requeue") {
          const consumerId = strParam(this.getNodeParameter("consumerId", i, "")).trim();
          const body: Record<string, unknown> = {};
          if (consumerId !== "") body["consumerId"] = consumerId;
          const response = toDataObject(
            await request.call(
              this,
              "POST",
              `/queues/${encode(queue)}/messages/${encode(messageId)}/${operation}`,
              body,
              i,
            ),
          );
          if (operation === "ack") {
            // Resolve a waiting "Specified Later in Workflow" trigger fast.
            // Best-effort: without a waiting trigger there is nothing to
            // resolve, and the HTTP acknowledgement above already settled.
            try {
              this.sendResponse({ ...items[i]?.json, acknowledged: true });
            } catch {
              // ignore — standalone acknowledgement already succeeded
            }
          }
          returnData.push({
            json: {
              queue,
              messageId,
              [operation === "ack" ? "acked" : "requeued"]: true,
              ...response,
            },
          });
        } else if (operation === "delete") {
          await request.call(
            this,
            "DELETE",
            `/queues/${encode(queue)}/messages/${encode(messageId)}`,
            undefined,
            i,
          );
          returnData.push({ json: { queue, messageId, deleted: true } });
        } else if (operation === "setTtl") {
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
