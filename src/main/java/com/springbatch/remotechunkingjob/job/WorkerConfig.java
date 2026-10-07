package com.springbatch.remotechunkingjob.job;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.springbatch.remotechunkingjob.dominio.Pessoa;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.jms.dsl.Jms;

@Profile("worker")
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
public class WorkerConfig {
    @Autowired
    private RemoteChunkingWorkerBuilder<Pessoa, Pessoa> pessoaWorkerBuilder;

    @Bean
    public IntegrationFlow integrationFlow(
            ItemProcessor<Pessoa, Pessoa> pessoaItemProcessor,
            ItemWriter<Pessoa> pessoaItemWriter) {
        return pessoaWorkerBuilder
                .itemProcessor(pessoaItemProcessor)
                .itemWriter(pessoaItemWriter)
                .inputChannel(requests())
                .outputChannel(replies())
                .build();
    }

    @Bean
    public DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow(ActiveMQConnectionFactory connectionFactory) {
        return IntegrationFlows
                .from(replies())
                .handle(Jms.outboundAdapter(connectionFactory).destination("replies"))
                .get();
    }

    @Bean
    public IntegrationFlow inboundFlow(ActiveMQConnectionFactory connectionFactory) {
        return IntegrationFlows
                .from(Jms.messageDrivenChannelAdapter(connectionFactory)
                        .destination("requests"))
                .channel(requests())
                .get();
    }
}
