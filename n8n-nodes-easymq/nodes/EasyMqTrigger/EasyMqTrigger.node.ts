import WebSocket from "ws";
import type {
  IDataObject,
  IExecuteResponsePromiseData,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  IRun,
  ITriggerFunctions,
  ITriggerResponse,
} from "n8n-workflow";
import { NodeConnectionTypes, NodeOperationError } from "n8n-workflow";

type AcknowledgeMode =
  "immediately" | "executionFinishes" | "executionFinishesSuccessfully" | "laterMessageNode";

interface TriggerOptions {
  acknowledge?: AcknowledgeMode;
  maxConcurrentExecutions?: number;
  visibilityTimeoutMs?: number;
  consumerId?: string;
}

interface EasyMqCredentials {
  baseUrl?: string;
  apiToken?: string;
}

interface DeliveredMessage {
  id: string;
  data: unknown;
  deliveryCount: number;
  redelivered: boolean;
}

interface ReadyInfo {
  consumerId: string;
}

/** A message the broker failed to settle because it is already gone. */
function isAlreadySettled(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const message = (error as { message?: unknown }).message;
  return (
    typeof message === "string" && (message.includes("NOT_FOUND") || message.includes("CONFLICT"))
  );
}

/** Narrow an unknown frame field to a string (never stringifies objects). */
function frameString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

/** Decode a WebSocket frame payload to text. */
function frameText(raw: WebSocket.RawData): string {
  if (typeof raw === "string") return raw;
  if (Array.isArray(raw)) return Buffer.concat(raw).toString("utf8");
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString("utf8");
  return raw.toString("utf8");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

export class EasyMqTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: "EasyMQ Trigger",
    name: "easyMqTrigger",
    icon: "file:easymq.svg",
    group: ["trigger"],
    version: 1,
    description: "Listens to easyMQ messages",
    defaults: {
      name: "EasyMQ Trigger",
    },
    triggerPanel: {
      header: "",
      executionsHelp: {
        inactive:
          "<b>While building your workflow</b>, click the 'execute step' button, then publish a message to the easyMQ queue. This will trigger an execution, which will show up in this editor.<br /> <br /><b>Once you're happy with your workflow</b>, publish it. Then every time a message arrives, the workflow will execute. These executions will show up in the <a data-key='executions'>executions list</a>, but not in the editor.",
        active:
          "<b>While building your workflow</b>, click the 'execute step' button, then publish a message to the easyMQ queue. This will trigger an execution, which will show up in this editor.<br /> <br /><b>Your workflow will also execute automatically</b>, since it's activated. Every time a message arrives, this node will trigger an execution. These executions will show up in the <a data-key='executions'>executions list</a>, but not in the editor.",
      },
      activationHint:
        "Once you've finished building your workflow, publish it to have it also listen continuously (you just won't see those executions here).",
    },
    inputs: [],
    outputs: [NodeConnectionTypes.Main],
    credentials: [
      {
        name: "easyMqApi",
        required: true,
      },
    ],
    properties: [
      {
        displayName: "Queue",
        name: "queue",
        type: "string",
        default: "",
        required: true,
        placeholder: "queue-name",
        description: "The name of the queue to listen to (declared automatically if missing)",
      },
      {
        displayName: "Options",
        name: "options",
        type: "collection",
        default: {},
        placeholder: "Add option",
        options: [
          {
            displayName: "Acknowledge",
            name: "acknowledge",
            type: "options",
            options: [
              {
                name: "Execution Finishes",
                value: "executionFinishes",
                description:
                  "After the workflow execution finished. No matter if the execution was successful or not.",
              },
              {
                name: "Execution Finishes Successfully",
                value: "executionFinishesSuccessfully",
                description: "After the workflow execution finished successfully",
              },
              {
                name: "Immediately",
                value: "immediately",
                description: "As soon as the message got received",
              },
              {
                name: "Specified Later in Workflow",
                value: "laterMessageNode",
                description: "Using an EasyMQ node to acknowledge the message",
              },
            ],
            default: "immediately",
            description: "When to acknowledge the message",
          },
          {
            displayName: "Consumer ID",
            name: "consumerId",
            type: "string",
            default: "",
            placeholder: "my-workflow",
            description:
              "Consumer identity for leases (generated when empty). Use a distinct id per workflow when several workflows share a queue",
          },
          {
            displayName: "Max Concurrent Executions",
            name: "maxConcurrentExecutions",
            type: "number",
            default: 1,
            displayOptions: {
              hide: {
                acknowledge: ["immediately"],
              },
            },
            description:
              "Max number of executions at a time. Messages beyond this wait in the queue until one finishes and acknowledges.",
          },
          {
            displayName: "Visibility Timeout (Ms)",
            name: "visibilityTimeoutMs",
            type: "number",
            default: 60000,
            description:
              "Lease per message: unacked past this timeout the message is redelivered. Keep it above your workflow runtime.",
          },
        ],
      },
      {
        displayName:
          "To acknowledge the message, insert an EasyMQ node later in the workflow and use the 'Acknowledge' operation",
        name: "laterMessageNode",
        type: "notice",
        displayOptions: {
          show: {
            "/options.acknowledge": ["laterMessageNode"],
          },
        },
        default: "",
      },
    ],
  };

  async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
    const queue = this.getNodeParameter("queue") as string;
    const options = this.getNodeParameter("options", {}) as TriggerOptions;

    let acknowledgeMode: AcknowledgeMode = options.acknowledge ?? "immediately";
    const maxConcurrent = options.maxConcurrentExecutions ?? 1;
    if (!Number.isInteger(maxConcurrent) || maxConcurrent < 1) {
      throw new NodeOperationError(
        this.getNode(),
        "Max Concurrent Executions must be an integer greater than zero.",
      );
    }
    if (maxConcurrent > 1 && acknowledgeMode === "immediately") {
      // Same rule as the RabbitMQ Trigger: a concurrency limit cannot be
      // combined with immediate acknowledgement, so the messages are
      // acknowledged when their executions finish instead.
      acknowledgeMode = "executionFinishes";
    }
    const visibilityTimeoutMs = options.visibilityTimeoutMs ?? 60000;
    const consumerId = (options.consumerId ?? "").trim() || undefined;
    // Flow control stays internal: the broker leases at most this many
    // messages to the consumer; the user only thinks in executions.
    const prefetch = acknowledgeMode === "immediately" ? 100 : maxConcurrent;

    const credentials = (await this.getCredentials("easyMqApi")) as unknown as EasyMqCredentials;
    const baseUrl = (credentials.baseUrl ?? "").replace(/\/+$/, "");
    const apiToken = credentials.apiToken ?? "";
    const wsUrl = `${baseUrl.replace(/^http/, "ws")}/queues/${encodeURIComponent(queue)}/subscribe`;

    // Idempotent declare so listening on a fresh queue does not fail.
    try {
      await this.helpers.requestWithAuthentication.call(this, "easyMqApi", {
        method: "PUT",
        baseURL: baseUrl,
        url: `/queues/${encodeURIComponent(queue)}`,
        body: {},
        json: true,
      });
    } catch (error) {
      throw new NodeOperationError(this.getNode(), describeApiError(error));
    }

    const inflight = new Set<string>();
    const pendingSettles = new Map<string, { resolve: () => void; reject: (err: Error) => void }>();
    let closeGotCalled = false;
    let socket: WebSocket | undefined;
    let activeConsumerId = "";

    const logError = (message: string): void => {
      const workflow = this.getWorkflow();
      const node = this.getNode();
      this.logger.error(
        `There was a problem with the EasyMQ Trigger node "${node.name}" in workflow "${workflow.id}": "${message}"`,
        { node: node.name, workflowId: workflow.id },
      );
    };

    const openSocket = async (): Promise<WebSocket> => {
      const candidate = new WebSocket(wsUrl, {
        headers: { Authorization: `Bearer ${apiToken}` },
      });
      return new Promise<WebSocket>((resolve, reject) => {
        const timer = setTimeout(() => {
          candidate.terminate();
          reject(new Error("Timed out connecting to easyMQ"));
        }, 15000);
        candidate.once("open", () => {
          clearTimeout(timer);
          resolve(candidate);
        });
        candidate.once("error", (err: Error) => {
          clearTimeout(timer);
          reject(err);
        });
      });
    };

    const waitReady = async (candidate: WebSocket): Promise<ReadyInfo> => {
      return new Promise<ReadyInfo>((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error("Timed out waiting for the easyMQ hello reply"));
        }, 10000);
        const cleanup = (): void => {
          clearTimeout(timer);
          candidate.off("message", onFrame);
          candidate.off("close", onClose);
        };
        const onFrame = (raw: WebSocket.RawData): void => {
          let frame: IDataObject;
          try {
            frame = JSON.parse(frameText(raw)) as IDataObject;
          } catch {
            return;
          }
          if (frame["type"] === "ready" && typeof frame["consumerId"] === "string") {
            cleanup();
            resolve({ consumerId: frame["consumerId"] });
          } else if (frame["type"] === "error") {
            cleanup();
            const code = frameString(frame["code"]) || "ERROR";
            const message = frameString(frame["message"]) || "subscribe failed";
            reject(new Error(`easyMQ ${code}: ${message}`));
          }
        };
        const onClose = (): void => {
          cleanup();
          reject(new Error("easyMQ connection closed before the hello reply"));
        };
        candidate.on("message", onFrame);
        candidate.once("close", onClose);
      });
    };

    /** Acknowledge/reject over the persistent connection. */
    const sendSettle = (action: "ack" | "requeue", id: string): Promise<void> => {
      return new Promise<void>((resolve, reject) => {
        const current = socket;
        if (!current || current.readyState !== WebSocket.OPEN) {
          reject(new Error("EasyMQ connection is not open"));
          return;
        }
        const timer = setTimeout(() => {
          pendingSettles.delete(id);
          reject(new Error("Timed out settling the easyMQ message"));
        }, 10000);
        pendingSettles.set(id, {
          resolve: () => {
            clearTimeout(timer);
            resolve();
          },
          reject: (err: Error) => {
            clearTimeout(timer);
            reject(err);
          },
        });
        try {
          current.send(JSON.stringify({ action, id }));
        } catch (error) {
          pendingSettles.delete(id);
          clearTimeout(timer);
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      });
    };

    const settleAck = async (id: string): Promise<void> => {
      try {
        await sendSettle("ack", id);
      } catch (error) {
        // Already settled elsewhere (e.g. an EasyMQ node in the workflow
        // acknowledged first) — nothing left to do.
        if (!isAlreadySettled(error)) throw error;
      }
    };

    const settleRequeue = async (id: string): Promise<void> => {
      try {
        await sendSettle("requeue", id);
      } catch (error) {
        if (!isAlreadySettled(error)) throw error;
      }
    };

    const routeReplyFrame = (frame: IDataObject): void => {
      const id = frame["id"];
      if (typeof id !== "string") return;
      const pending = pendingSettles.get(id);
      if (!pending) return;
      pendingSettles.delete(id);
      if (frame["type"] === "acked" || frame["type"] === "requeued") {
        pending.resolve();
      } else if (frame["type"] === "error") {
        const code = frameString(frame["code"]) || "ERROR";
        const message = frameString(frame["message"]) || "settle failed";
        pending.reject(new Error(`easyMQ ${code}: ${message}`));
      }
    };

    const toItem = (message: DeliveredMessage): INodeExecutionData => ({
      json: {
        queue,
        messageId: message.id,
        consumerId: activeConsumerId,
        data: message.data as IDataObject,
        deliveryCount: message.deliveryCount,
        redelivered: message.redelivered,
      },
    });

    const handleMessage = async (message: DeliveredMessage): Promise<void> => {
      if (closeGotCalled) return;
      inflight.add(message.id);
      try {
        const item = toItem(message);
        if (acknowledgeMode === "immediately") {
          this.emit([[item]]);
          await settleAck(message.id);
          return;
        }
        if (acknowledgeMode === "laterMessageNode") {
          const responsePromiseHook =
            this.helpers.createDeferredPromise<IExecuteResponsePromiseData>();
          // Also await execution end so a failed run requeues even when no
          // EasyMQ node fires sendResponse first.
          const responsePromise = this.helpers.createDeferredPromise<IRun>();
          this.emit([[item]], responsePromiseHook, responsePromise);
          type RaceResult = { kind: "hook"; real: boolean } | { kind: "run"; data: IRun };
          const first = await Promise.race<RaceResult>([
            responsePromiseHook.promise.then((data): RaceResult => ({
              kind: "hook",
              real:
                data !== null && typeof data === "object" && Object.keys(data as object).length > 0,
            })),
            responsePromise.promise.then((data): RaceResult => ({ kind: "run", data })),
          ]);
          if (first.kind === "hook" && first.real) {
            await settleAck(message.id);
          } else {
            const run = first.kind === "run" ? first.data : await responsePromise.promise;
            if (run?.data?.resultData?.error) {
              await settleRequeue(message.id);
            } else {
              await settleAck(message.id);
            }
          }
          return;
        }
        const responsePromise = this.helpers.createDeferredPromise<IRun>();
        this.emit([[item]], undefined, responsePromise);
        const run = await responsePromise.promise;
        if (run?.data?.resultData?.error) {
          if (acknowledgeMode === "executionFinishesSuccessfully") {
            await settleRequeue(message.id);
            return;
          }
        }
        await settleAck(message.id);
      } catch (error) {
        logError(error instanceof Error ? error.message : String(error));
      } finally {
        inflight.delete(message.id);
      }
    };

    const onSocketFrame = (raw: WebSocket.RawData): void => {
      let frame: IDataObject;
      try {
        frame = JSON.parse(frameText(raw)) as IDataObject;
      } catch {
        return;
      }
      if (frame["type"] === "message" && typeof frame["id"] === "string") {
        void handleMessage({
          id: frame["id"],
          data: frame["data"],
          deliveryCount: typeof frame["deliveryCount"] === "number" ? frame["deliveryCount"] : 1,
          redelivered: frame["redelivered"] === true,
        });
        return;
      }
      if (frame["type"] === "acked" || frame["type"] === "requeued" || frame["type"] === "error") {
        routeReplyFrame(frame);
        if (frame["type"] === "error" && frame["id"] === undefined) {
          const code = frameString(frame["code"]) || "ERROR";
          const message = frameString(frame["message"]);
          logError(message === "" ? `easyMQ ${code}` : `easyMQ ${code}: ${message}`);
        }
      }
    };

    // The "closeFunction" gets called by n8n whenever the workflow gets
    // deactivated and can so clean up.
    const closeFunction = async (): Promise<void> => {
      closeGotCalled = true;
      // Give in-flight executions a bounded grace period before the
      // connection closes; leftovers requeue server-side on close.
      let waits = 0;
      while (inflight.size > 0 && waits++ < 60) {
        await sleep(1000);
      }
      for (const [, pending] of pendingSettles) {
        pending.reject(new Error("EasyMQ Trigger is closing"));
      }
      pendingSettles.clear();
      const current = socket;
      socket = undefined;
      if (current) {
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, 5000);
          current.once("close", () => {
            clearTimeout(timer);
            resolve();
          });
          try {
            current.close(1000, "Trigger deactivated");
          } catch {
            clearTimeout(timer);
            resolve();
          }
        });
      }
    };

    const connectConsumer = async (
      helloPrefetch: number,
      attachMessageHandler: boolean,
    ): Promise<void> => {
      const candidate = await openSocket();
      socket = candidate;
      candidate.send(
        JSON.stringify({
          action: "hello",
          ...(consumerId !== undefined ? { consumerId } : {}),
          prefetch: helloPrefetch,
          visibilityTimeoutMs,
        }),
      );
      const ready = await waitReady(candidate);
      activeConsumerId = ready.consumerId;
      if (attachMessageHandler) {
        candidate.on("message", onSocketFrame);
      }
      candidate.on("error", (err: Error) => {
        logError(err.message);
      });
      candidate.on("close", (code: number) => {
        if (socket === candidate) socket = undefined;
        for (const [, pending] of pendingSettles) {
          pending.reject(new Error("EasyMQ connection closed"));
        }
        pendingSettles.clear();
        if (!closeGotCalled) {
          this.emitError(new Error(`EasyMQ connection closed unexpectedly (code ${String(code)})`));
        }
      });
    };

    if (this.getMode() === "manual") {
      const manualTriggerFunction = async (): Promise<void> => {
        // Catch a single message for the editor test run, then disconnect.
        // The active frame handler stays detached so the test run settles
        // the message itself, immediately, without execution hooks.
        await connectConsumer(1, false);
        const first = await new Promise<DeliveredMessage | undefined>((resolve) => {
          const current = socket;
          if (!current) {
            resolve(undefined);
            return;
          }
          const onFrame = (raw: WebSocket.RawData): void => {
            let frame: IDataObject;
            try {
              frame = JSON.parse(frameText(raw)) as IDataObject;
            } catch {
              return;
            }
            if (frame["type"] === "message" && typeof frame["id"] === "string") {
              current.off("message", onFrame);
              resolve({
                id: frame["id"],
                data: frame["data"],
                deliveryCount:
                  typeof frame["deliveryCount"] === "number" ? frame["deliveryCount"] : 1,
                redelivered: frame["redelivered"] === true,
              });
            }
          };
          current.on("message", onFrame);
        });
        if (first) {
          this.emit([[toItem(first)]]);
          await settleAck(first.id).catch((error: unknown) => {
            logError(error instanceof Error ? error.message : String(error));
          });
        }
        await closeFunction();
      };

      return {
        closeFunction,
        manualTriggerFunction,
      };
    }

    await connectConsumer(prefetch, true);

    return {
      closeFunction,
    };
  }
}
