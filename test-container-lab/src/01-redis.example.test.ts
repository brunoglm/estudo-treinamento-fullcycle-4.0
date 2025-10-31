import { GenericContainer } from 'testcontainers';
import { createClient } from 'redis'
it("deve armazenar e recuperar dados do Redis usando Testcontainers", async () => {
  // Criar e iniciar o contêiner do Redis
  const setupStart = performance.now();
  const redisContainer = await new GenericContainer('redis:7-alpine')
    .withExposedPorts(6379)
    .start();
  console.log(`Tempo de configuração do contêiner Redis: ${(performance.now() - setupStart).toFixed(2)} ms`);

  // Configurar o cliente Redis para se conectar ao contêiner
  const startTest = performance.now();
  const url = `redis://${redisContainer.getHost()}:${redisContainer.getMappedPort(6379)}`;
  console.log(`Conectando ao Redis em: ${url}`);
  const redisClient = createClient({
    url,
  });
  await redisClient.connect();

  await redisClient.set('chaveTeste', 'valorTeste');

  // Recuperar o valor do Redis
  const valorRecuperado = await redisClient.get('chaveTeste');

  // Verificar se o valor recuperado está correto
  expect(valorRecuperado).toBe('valorTeste');

  // Limpar recursos
  await redisClient.quit();
  console.log(`Teste executado em: ${(performance.now() - setupStart).toFixed(2)} ms`);

  const cleanupStart = performance.now();
  await redisContainer.stop({ remove: false });
  console.log(`Cleanup completo em: ${(performance.now() - cleanupStart).toFixed(2)} ms`);
});
