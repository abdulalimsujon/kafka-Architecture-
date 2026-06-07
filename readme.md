# Kafka Notes

## Remaining Topics

- [ ] Offset
- [ ] groupId / Consumer group
- [ ] Partition
- [ ] Read Doc from Apache Kafka

---

## ১. সহজ উদাহরণ: পিজ্জা ডেলিভারি টিম

একদম সহজ (Extreme Beginner Friendly) ভাবে বোঝার জন্য আমরা একটি **"পিজ্জা ডেলিভারি টিম"**-এর উদাহরণ ব্যবহার করতে পারি।

কল্পনা করুন, একটি বড় ***পিজ্জা শপ*** আছে যেখান থেকে অনেক পিজ্জা *ডেলিভারি* দিতে হবে।

- **Kafka Topic:** এটি হলো পিজ্জার *অর্ডার লিস্ট* (একটার পর একটা অর্ডার আসছে)।
- **Consumer:** এটি হলো আপনার *ডেলিভারি বয়* (আপনার লেখা কোড)।
- **groupId:** এটি হলো ডেলিভারি **"টিমের নাম"**।

---

## ২. ডায়াগ্রাম (সহজ চিত্র)

```
[ পিজ্জা অর্ডারের লিস্ট ]  <-- (Kafka Topic)
    |  অর্ডার ১
    |  অর্ডার ২
    |  অর্ডার ৩
    |  অর্ডার ৪
    v
---------------------------
|   টিম: "analytic-service" |  <-- (আপনার groupId)
---------------------------
      /           \
 [ডেলিভারি বয় ১]  [ডেলিভারি বয় ২]  <-- (একই কোড দুই জায়গায় চলছে)
     |               |
(অর্ডার ১ ও ২ নেয়) (অর্ডার ৩ ও ৪ নেয়)
```

---

## ৩. `groupId` কেন দরকার? (৩টি সহজ কারণ)

### ক) কাজ ভাগ করে নেওয়া (Teamwork)

যদি আপনার অনেকগুলো অর্ডার থাকে, তবে একজন ডেলিভারি বয়ের জন্য খুব কষ্ট হবে। আপনি যদি একই **`groupId`** দিয়ে ২ জন ডেলিভারি বয় নিয়োগ করেন, তবে Kafka নিজে থেকেই কাজ ভাগ করে দেবে।

**রেজাল্ট:** ১ নম্বর বয় পিজ্জা ১ নিলে, ২ নম্বর বয় নিজে থেকেই পিজ্জা ২ নিবে। কেউ কারো কাজে ডিস্টার্ব করবে না।

### খ) "বুকমার্ক" বা মনে রাখা (The Bookmark)

ধরুন, ডেলিভারি বয় কাজ করতে করতে ক্লান্ত হয়ে বিরতি নিল। যখন সে আবার ফিরে আসবে, সে কীভাবে জানবে কয়টা ডেলিভারি শেষ হয়েছে?

Kafka ওই **`groupId`**-এর নামে একটি খাতায় লিখে রাখে: *"analytic-service টিম, ৫ নম্বর পিজ্জা পর্যন্ত ডেলিভারি দিয়েছে"*।

**রেজাল্ট:** পরের বার কাজ শুরু করলে সে ৬ নম্বর থেকেই শুরু করবে।

### গ) নতুন গ্রুপ আসলে কী হয়?

যদি নতুন একটি টিম আসে যার নাম **`groupId: "marketing-service"`**, তবে Kafka তাদের জন্য আলাদা একটি খাতা খুলবে। তারা আবার একদম ১ নম্বর পিজ্জা থেকেই ডেলিভারি দেওয়া শুরু করবে। কারণ তারা সম্পূর্ণ নতুন টিম।

---

## সারসংক্ষেপ

- **একই `groupId`** = তারা একটি টিমের মতো কাজ করে (একই কাজ দুইজন করে না, কাজ ভাগ করে নেয়)।
- **ভিন্ন `groupId`** = তারা আলাদা আলাদা টিম (সবাই সব কাজ শুরু থেকে শেষ পর্যন্ত করতে চায়)।

> **আপনার কোডে `groupId: "analytic-service"` দেওয়ার মানে হলো আপনি আপনার সার্ভিসকে একটি নির্দিষ্ট নাম দিচ্ছেন যেন Kafka তার কাজের হিসাব (বুকমার্ক) মনে রাখতে পারে।**

---

# ClientId & Brokers [Kafka]

```ts
this.kafka = new Kafka({
  clientId: 'make-clientId',
  brokers: ['localhost:9092'],
});
```

Here you are creating a **Kafka client** using **KafkaJS**, which is a Node.js library for working with **Apache Kafka**.

Two important properties are used here:

- `clientId`
- `brokers`

---

## 1️⃣ What `clientId` Actually Does

```ts
clientId: 'make-clientId'
```

`clientId` is the **name/identity of your application when talking to Kafka**. Think of it like a **username for your service** when it connects to Kafka.

### Why Kafka needs it

**1. Logging & Debugging**

If something goes wrong in Kafka logs, you will see something like:

```
[Kafka] clientId=billing-service connected
[Kafka] clientId=business-service produced message
```

So you immediately know **which service sent the message**.

| Service | clientId |
| --- | --- |
| Billing Service | `billing-service` |
| Business Service | `business-service` |
| Notification Service | `notification-service` |

**2. Monitoring & Metrics**

Kafka tracks metrics per client such as request latency, message throughput, and errors.

Monitoring tools like **Prometheus** and **Grafana** can show metrics like:

```
clientId=billing-service
requests/sec = 120
```

**3. Broker-side Request Tracking**

Every request to Kafka broker includes the `clientId`:

```
ProduceRequest
clientId: billing-service
topic: billing-events
```

### Real Micro-service Example

Producer (Business Service):

```ts
const kafka = new Kafka({
  clientId: 'business-service',
  brokers: ['localhost:9092'],
});
```

Consumer (Billing Service):

```ts
const kafka = new Kafka({
  clientId: 'billing-service',
  brokers: ['localhost:9092'],
});
```

Now Kafka logs become **very readable**.

---

## 2️⃣ What `brokers` Actually Does

```ts
brokers: ['localhost:9092']
```

`brokers` = **Kafka server addresses your app connects to**.

Your app must know: *"Where is Kafka running?"* — so you provide the **broker addresses**.

### What is a Kafka Broker?

A **broker** is simply a **Kafka server**.

```
Kafka Cluster
   │
   ├── Broker 1 → localhost:9092
   ├── Broker 2 → localhost:9093
   └── Broker 3 → localhost:9094
```

Each broker stores messages, manages partitions, and handles producer & consumer requests.

### Why `brokers` is an Array

```ts
brokers: [
  'kafka1:9092',
  'kafka2:9092',
  'kafka3:9092'
]
```

Because Kafka usually runs as a **cluster**. Your app only needs **one broker to connect**, then Kafka automatically discovers the rest:

```
App → connect to kafka1
kafka1 → returns cluster metadata
App → learns about kafka2 and kafka3
```

**Local development:**

```
localhost:9092  →  Kafka server running on your computer, port 9092
```

---

## 3️⃣ What Happens When This Code Runs

```ts
this.kafka = new Kafka({
  clientId: 'billing-service',
  brokers: ['localhost:9092'],
});
```

**Step-by-step:**

1. Your app starts
2. KafkaJS creates a **Kafka client**
3. Client connects to **broker localhost:9092**
4. Broker sends **cluster metadata**

Example metadata:

```
Topics:
- business-events
- billing-events

Partitions:
- billing-events partition 0 → broker1
- billing-events partition 1 → broker2
```

Now your client knows **the full Kafka cluster**.

---

## 4️⃣ Real Production Example

```ts
const kafka = new Kafka({
  clientId: 'billing-service',
  brokers: [
    'kafka1:9092',
    'kafka2:9092',
    'kafka3:9092',
  ],
});
```

If one broker dies:

```
kafka1 ❌
kafka2 ✅
kafka3 ✅
```

Your service **still works**. That's why Kafka is **fault tolerant**.

> একাধিক instances যখন একই groupId ব্যবহার করে, kafka এদেরকে one team হিসেবে বিবেচনা করে।

---

# The Full Life-Cycle of Kafka [STORY]

## Producer Story

When your app runs, at the very beginning the `Constructor` starts working.

**Step 1 — Kafka client runs inside the constructor:**

```ts
this.kafka = KafkaClientFactory.create(configservice);
```

This kafka-client holds the kafka-server address which contains `kafka-broker[cluster]` and `ClientId`. It works as a bridge between your `app` and `kafka-server`.

```ts
export class KafkaClientFactory {
  static create(configService: ConfigService): Kafka {
    const config = getKafkaConfig(configService);

    return new Kafka({
      clientId: config.clientId,
      brokers: config.brokers!,
      retry: config.retry,
    });
  }
}
```

The kafka-server can be run in different places like Docker.

**Step 2 — Kafka producer instance is created:**

```ts
// Instance of kafka-producer
this.producer = this.kafka.producer({
  maxInFlightRequests: 1, // How many requests can be processed at same time
  idempotent: true,       // No duplicate message allowed even if retries
  transactionTimeout: 3000,
});
```

**Step 3 — App tries to connect with the kafka-server** based on this kafka-producer instance. The kafka-client provides the address of the kafka-server.

**Step 4 — After successfully connecting, you can send a message from any service:**

```ts
// Publish subscription event to Kafka
await this.KafkaProducerService.publishSubscriptionEvent({
  type: 'SUBSCRIBED',
  businessInfo,
  serviceId,
  subscriptionStatus: isHold ? 'HOLD' : 'ACTIVE',
  timestamp: new Date().toISOString(),
});
```

**Step 5 — Kafka producer sends the message to the kafka-server:**

```ts
async publishSubscriptionEvent(event: SubscriptionEvent): Promise<void> {
  await this.producer.send({
    topic,
    messages: [
      {
        key: `${event.businessInfo._id}-${event.serviceId}`,
        value: JSON.stringify(event),
        timestamp: new Date().getTime().toString(),
      },
    ],
  });
}
```

---

## Consumer Story

**Step 1 — Kafka client runs inside the constructor:**

```ts
this.kafka = KafkaClientFactory.create(configservice);
```

Same as the producer — the kafka-client works as a bridge between your app and kafka-server.

**Step 2 — Kafka consumer instance is created:**

```ts
this.consumer = this.kafka.consumer({
  groupId: configservice.get<string>('', ''),
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 1048576, // 1MB
  retry: {
    initialRetryTime: 300,
    retries: 5,
    multiplier: 2,
    maxRetryTime: 30000,
  },
});
```

**Step 3 — App tries to connect with the kafka-server.**

**Step 4 — After successfully connecting, you can pull messages from the kafka-server.**

**Step 5 — Subscribe to at least one topic:**

```ts
/* Basic syntax of consumer subscription */
consumer.subscribe({
  topic?: string,
  topics?: string[],
  fromBeginning: Boolean
})
```

```ts
/* Real life example */
async subscribeTopics() {
  this.consumer.subscribe({
    topic: "user-created",
    topics: ["notification-service", "email-service"],
    fromBeginning: false
  })
}
```

> **Meaning:** "I want messages from `user.created`, `notification-service`, `email-service` topic. `fromBeginning: false` means I don't want to see messages from the start."
>
> `offset` is a message tracker — it tracks which offset you already read (offset-0, offset-1).

After subscribing to the topic, Kafka does **NOT** start consuming immediately.

**Step 6 — Actual polling begins when you run the consumer:**

```ts
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log(message.value?.toString());
  },
});
```

---

# Partition

Kafka-producer যখন message send করে same key দিয়ে, তখন same partition-এ message store হয়। যদি key different হয় তাহলে partition আলাদা হয়।

The producer decides the target partition to place any message depending on:

- **Partition id** — if it's specified within the message
- **key % num partitions** — if no partition id is mentioned
- **Round robin** — if neither partition id nor message key is available (only value is present)
