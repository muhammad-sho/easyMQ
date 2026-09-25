import type {
  IDataObject,
  IHttpRequestMethods,
  NodeParameterValueType,
  INodeType,
  INodeTypeDescription,
  IPollFunctions,
  INodeExecutionData,
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

function boolParam(value: NodeParameterValueType | object, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

const POLL_TIMES: INodeTypeDescription["properties"] = [
  {
    displayName: "Poll Times",
    name: "pollTimes",
    type: "fixedCollection",
    typeOptions: {
      multipleValues: true,
    },
    default: {},
    description: "How often the queue should be polled for new messages",
    options: [
      {
        name: "item",
        displayName: "Poll Time",
        values: [
          {
            displayName: "Mode",
            name: "mode",
            type: "options",
            options: [
              { name: "Every Minute", value: "everyMinute" },
              { name: "Every Hour", value: "everyHour" },
              { name: "Every Day", value: "everyDay" },
              { name: "Every Week", value: "everyWeek" },
              { name: "Every Month", value: "everyMonth" },
              { name: "Every X", value: "everyX" },
              { name: "Custom", value: "custom" },
            ],
            default: "everyMinute",
            description: "How often to poll the queue",
          },
          {
            displayName: "Value",
            name: "value",
            type: "number",
            default: 2,
            displayOptions: { show: { mode: ["everyX"] } },
            description: "How many X (minutes/hours) between polls",
          },
          {
            displayName: "Unit",
            name: "unit",
            type: "options",
            default: "minutes",
            displayOptions: { show: { mode: ["everyX"] } },
            options: [
              { name: "Minutes", value: "minutes" },
              { name: "Hours", value: "hours" },
            ],
          },
          {
            displayName: "Hour",
            name: "hour",
            type: "number",
            default: 9,
            displayOptions: { show: { mode: ["everyDay", "everyWeek", "everyMonth"] } },
            description: "The hour of the day to poll (0-23)",
          },
          {
            displayName: "Minute",
            name: "minute",
            type: "number",
            default: 0,
            displayOptions: { show: { mode: ["everyDay", "everyWeek", "everyMonth"] } },
            description: "The minute of the hour to poll (0-59)",
          },
          {
            displayName: "Weekday",
            name: "weekday",
            type: "options",
            default: "monday",
            displayOptions: { show: { mode: ["everyWeek"] } },
            options: [
              { name: "Monday", value: "monday" },
              { name: "Tuesday", value: "tuesday" },
              { name: "Wednesday", value: "wednesday" },
              { name: "Thursday", value: "thursday" },
              { name: "Friday", value: "friday" },
              { name: "Saturday", value: "saturday" },
              { name: "Sunday", value: "sunday" },
            ],
          },
          {
            displayName: "Day of Month",
            name: "dayOfMonth",
            type: "number",
            default: 1,
            displayOptions: { show: { mode: ["everyMonth"] } },
            description: "The day of the month to poll (1-31)",
          },
          {
            displayName: "Cron Expression",
            name: "cron",
            type: "string",
            default: "*/5 * * * *",
            displayOptions: { show: { mode: ["custom"] } },
            description: "Custom cron expression for polling",
          },
        ],
      },
    ],
  },
];

export class EasyMqTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: "EasyMQ Trigger",
    name: "easyMqTrigger",
    icon: "file:easymq.svg",
    group: ["trigger"],
    version: 1,
    description: "Consume messages from an easyMQ queue",
    defaults: {
      name: "EasyMQ Trigger",
    },
    inputs: [],
    outputs: [NodeConnectionTypes.Main],
    credentials: [
      {
        name: "easyMqApi",
        required: true,
      },
    ],
    polling: true,
    properties: [
      {
        displayName: "Queue",
        name: "queue",
        type: "string",
        default: "",
        required: true,
        description: "Name of the queue to consume (declared automatically if missing)",
      },
      {
        displayName: "Batch Size",
        name: "batchSize",
        type: "number",
        default: 10,
        description: "Max messages to fetch per poll (consumer concurrency = parallel polls)",
      },
      {
        displayName: "Consumer ID",
        name: "consumerId",
        type: "string",
        default: "n8n-trigger",
        description:
          "Consumer identity for leases and prefetch — use a distinct id per workflow when several workflows share a queue",
      },
      {
        displayName: "Prefetch",
        name: "prefetch",
        type: "number",
        default: 100,
        description: "Max messages leased to this consumer at once",
      },
      {
        displayName: "Visibility Timeout (Ms)",
        name: "visibilityTimeoutMs",
        type: "number",
        default: 60000,
        description:
          "Lease per message: unacked past this timeout the message is redelivered. Keep it above your workflow runtime when Auto Acknowledge is on.",
      },
      {
        displayName: "Auto Acknowledge",
        name: "autoAck",
        type: "boolean",
        default: true,
        description:
          "Whether to acknowledge messages after they are emitted. Turn off to ack manually with the EasyMQ node (message id is $json.id).",
      },
      ...POLL_TIMES,
    ],
  };

  async poll(this: IPollFunctions): Promise<INodeExecutionData[][] | null> {
    const queue = strParam(this.getNodeParameter("queue", ""));
    const batchSize = numParam(this.getNodeParameter("batchSize", 10), 10);
    const consumerId = strParam(
      this.getNodeParameter("consumerId", "n8n-trigger"),
      "n8n-trigger",
    ).trim();
    const prefetch = numParam(this.getNodeParameter("prefetch", 100), 100);
    const visibilityTimeoutMs = numParam(
      this.getNodeParameter("visibilityTimeoutMs", 60000),
      60000,
    );
    const autoAck = boolParam(this.getNodeParameter("autoAck", true), true);

    // Idempotent declare so polling a fresh queue does not 404.
    await apiRequest.call(this, "PUT", `/queues/${encodeURIComponent(queue)}`, {});

    const response = toDataObject(
      await apiRequest.call(this, "POST", `/queues/${encodeURIComponent(queue)}/consume`, {
        consumerId: consumerId === "" ? undefined : consumerId,
        count: batchSize,
        visibilityTimeoutMs,
        prefetch,
      }),
    );

    const messages = toMessageList(response["messages"]);
    if (messages.length === 0) {
      return null;
    }

    const owner = typeof response["consumerId"] === "string" ? response["consumerId"] : consumerId;
    const items: INodeExecutionData[] = messages.map((message) => ({
      json: toDataObject({
        id: message["id"],
        queue,
        data: message["data"],
        deliveryCount: message["deliveryCount"],
        redelivered: message["redelivered"],
        visibleAt: message["visibleAt"],
      }),
    }));

    if (autoAck) {
      for (const message of messages) {
        const id = message["id"];
        if (typeof id !== "string") continue;
        await apiRequest.call(
          this,
          "POST",
          `/queues/${encodeURIComponent(queue)}/messages/${encodeURIComponent(id)}/ack`,
          owner === "" ? {} : { consumerId: owner },
        );
      }
    }

    return [items];
  }
}

function toMessageList(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return [];
  return value.filter(
    (entry): entry is Record<string, unknown> => typeof entry === "object" && entry !== null,
  );
}

async function apiRequest(
  this: IPollFunctions,
  method: IHttpRequestMethods,
  path: string,
  body: Record<string, unknown>,
): Promise<unknown> {
  const credentials = (await this.getCredentials("easyMqApi")) as unknown as {
    baseUrl?: string;
  };
  const baseUrl = (credentials.baseUrl ?? "").replace(/\/+$/, "");
  try {
    return await this.helpers.requestWithAuthentication.call(this, "easyMqApi", {
      method,
      baseURL: baseUrl,
      url: path,
      body,
      json: true,
    });
  } catch (error) {
    throw new NodeOperationError(this.getNode(), describeApiError(error));
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
