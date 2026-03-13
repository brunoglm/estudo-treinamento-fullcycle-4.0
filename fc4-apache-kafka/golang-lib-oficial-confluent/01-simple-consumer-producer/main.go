package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"simple-consumer-producer/consumer"
	"simple-consumer-producer/models"
	"simple-consumer-producer/producer"
	"syscall"
	"time"
)

func main() {
	addr := "localhost:9092"
	mainTopic := "usuarios_novos"
	dlqTopic := mainTopic + "-dlq"

	// 1. Iniciar o Produtor (usado para DLQ e envios normais)
	p, err := producer.NewProducer(addr)
	if err != nil {
		log.Fatalf("Erro ao criar produtor: %v", err)
	}
	defer p.Close()

	// 2. Iniciar o Consumidor injetando o produtor para suporte a DLQ
	c, err := consumer.NewConsumer(addr, "grupo-processador-usuarios", p)
	if err != nil {
		log.Fatalf("Erro ao criar consumidor: %v", err)
	}
	defer c.Close()

	cDLQ, err := consumer.NewConsumer(addr, "grupo-processador-usuarios-dlq", p)
	if err != nil {
		log.Fatalf("Erro ao criar consumidor dlq: %v", err)
	}
	defer cDLQ.Close()

	// 3. Canal para capturar sinais do SO (Ctrl+C, SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 4. Rodar o consumo em uma Goroutine para não travar o main
	go func() {
		c.ReadUsersWithWorkerPoolAndPushDLQ(mainTopic, 10, func(user models.UserEvent) error {
			// --- LÓGICA DE NEGÓCIO ---
			fmt.Printf(" [Worker] Processando: %s\n", user.Name)

			// Exemplo de erro que mandaria para a DLQ
			if user.ID == 0 {
				return errors.New("ID de usuário inválido")
			}

			time.Sleep(100 * time.Millisecond) // Simula carga
			return nil
		})
	}()

	// 5. OPCIONAL: Consumidor específico para a DLQ
	// Em produção, isso pode ser um processo separado que alerta o time.
	go func() {
		fmt.Println("🕵️ Monitor de DLQ ativo...")
		cDLQ.ReadUsers(dlqTopic, func(user models.UserEvent) {
			fmt.Printf("🚨 Alerta DLQ: Falha crítica no processamento do User %d\n", user.ID)
		})
	}()

	fmt.Println("🟢 Sistema operando. Pressione Ctrl+C para encerrar.")

	// Bloqueia o main aqui até receber um sinal de saída
	<-sigChan

	fmt.Println("\n🟡 Encerrando graciosamente...")
	// O defer p.Close() e c.Close() serão chamados agora
}

// func main() {
// 	// Definindo as flags
// 	offset := flag.Int64("offset", 0, "Offset da mensagem")
// 	partition := flag.Int("partition", 0, "Partição da mensagem")
// 	topic := flag.String("topic", "usuarios_novos", "Nome do tópico")

// 	flag.Parse()

// 	// Criamos um group-id aleatório para garantir que é temporário
// 	tempGroupID := fmt.Sprintf("debug-tool-%d", time.Now().Unix())

// 	c, _ := consumer.NewConsumer("localhost:9092", tempGroupID, nil)
// 	defer c.Close()

// 	// Chama sua função de Seek/Assign
// 	c.ReprocessSpecificOffset(*topic, int32(*partition), *offset, func(user models.UserEvent) {
// 		fmt.Printf("🛠️ Reprocessando usuário: %s\n", user.Name)
// 		// Aqui você pode chamar a mesma lógica de negócio da sua API
// 	})
// }

// Dry Run: No seu script de suporte, adicione uma flag -dry-run. Se for true, o script apenas imprime o que faria (ex: "Eu iria atualizar o saldo do usuário X para Y"), mas não executa a ação no banco de dados. Isso evita erros humanos em produção.

// Logs Extras: Como é um script de debug, habilite o log de debug da biblioteca Confluent para ver exatamente a comunicação com o broker:

// Encadeamento: Se você precisar reprocessar um range (ex: do offset 100 ao 200), basta mudar o ReadMessage para um loop for simples que para quando atingir o offset final desejado.
