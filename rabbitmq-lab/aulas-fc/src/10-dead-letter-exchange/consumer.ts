import amqp from "amqplib";

async function deadLetterExchange() {
  const connection = await amqp.connect("amqp://admin:admin@localhost:5672");
  const channel = await connection.createChannel();

  const mainQueue = "nfe.queue";
  const failQueue = "fail.queue";
  const retryQueue = "retry.queue";
  const mainExchange = "amq.direct";
  const retryExchange = "dlx.exchange";

  await channel.assertExchange(mainExchange, "direct");
  await channel.assertExchange(retryExchange, 'direct');

  await channel.assertQueue(mainQueue, {
    deadLetterExchange: retryExchange,
  });
  await channel.assertQueue(retryQueue, {
    'messageTtl': 5000,
    deadLetterExchange: mainExchange,
  });
  await channel.assertQueue(failQueue);


  await channel.bindQueue(mainQueue, mainExchange, 'order');
  await channel.bindQueue(retryQueue, retryExchange, 'order');

  console.log(`[*] Waiting for messages in ${mainQueue}. To exit press CTRL+C`);

  channel.consume(
    mainQueue,
    (msg) => {
      // Simular processamento, apenas para fins didático
      const content = msg?.content.toString();
      if (!msg || !content) {
        console.log("[!] Received empty message, ignoring...");
        if (msg) {
          const newMsg = { error: 'Received empty message', payload: '' };
          channel.sendToQueue(failQueue, Buffer.from(JSON.stringify(newMsg)));
          channel.ack(msg);
        }
        return;
      }

      console.log(`[x] Received '${content}'`);

      try {
        // Simular sucesso ou falha
        if (parseInt(content) > 5) {
          throw new Error("Processing failed");
        }

        console.log("[x] Done processing");
        channel.ack(msg);
      } catch (error) {
        //se aconteceu um erro não reprocessável, publicar na fila de falha

        const maxRetries = 3;
        const xDeath = msg.properties.headers?.["x-death"] || [];
        const retryCount = xDeath[0]?.count || 0;
        if (retryCount < maxRetries) {
          channel.nack(msg, false, false); //channel.reject(msg, false);
          console.log(`[!] Retrying message, retry count: ${retryCount}`);
        } else {
          //@ts-expect-error
          const newMsg = { error: error.message, payload: content };
          channel.sendToQueue(failQueue, Buffer.from(JSON.stringify(newMsg)));
          console.log(`[!] Sending message to fail queue`);
          channel.ack(msg);
        }

        //@ts-expect-error
        console.error("[!] Processing error:", error.message);


      }
    },
    { noAck: false }
  );
}

deadLetterExchange().catch(console.error);
