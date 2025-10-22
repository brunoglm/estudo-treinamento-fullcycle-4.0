import amqp from "amqplib";

async function producer() {
  const connection = await amqp.connect("amqp://admin:admin@localhost:5672");
  const channel = await connection.createChannel();

  const queueHello = "hello";
  const queueProducts = "products";

  await channel.assertQueue(queueHello);
  await channel.assertQueue(queueProducts);

  await channel.bindQueue(queueHello, "amq.fanout", "");
  await channel.bindQueue(queueProducts, "amq.fanout", "");

  //binding tem que ser feito na interface do rabbitmq management com a exchange fanout

  const messages = new Array(100).fill(0).map((_, i) => ({
    id: i,
    name: `Product ${i}`,
    price: Math.floor(Math.random() * 100),
  }));

  await Promise.all(
    messages.map((message) => {
      return channel.publish(
        "amq.fanout",
        "",
        Buffer.from(JSON.stringify(message)),
        {
          contentType: "application/json",
          deliveryMode: 2,
        }
      );
    })
  );
  console.log(`[x] Sent messages`);

  setTimeout(() => {
    connection.close();
    process.exit(0);
  }, 500);
}

producer().catch(console.error);
