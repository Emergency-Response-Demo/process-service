package com.redhat.cajun.navy.process;

import java.util.Collection;

import org.jbpm.kie.services.impl.CustomIdKModuleDeploymentUnit;
import org.jbpm.services.api.DeploymentService;
import org.jbpm.services.api.RuntimeDataService;
import org.jbpm.services.api.model.ProcessDefinition;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.query.QueryContext;
import org.kie.internal.runtime.conf.RuntimeStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan
@EnableAutoConfiguration(exclude = { KafkaAutoConfiguration.class })
public class ProcessServiceApplication {

    private final static Logger log = LoggerFactory.getLogger(ProcessServiceApplication.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ProcessServiceApplication.class);
        application.setRegisterShutdownHook(false);
        ConfigurableApplicationContext context = application.run(args);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdownhook called");
            context.close();
        }));
    }

    @Bean
    CommandLineRunner deployAndValidate() {
        return new CommandLineRunner() {

            @Value("${incident.deployment.id}")
            private String deploymentId;

            @Autowired
            private DeploymentService deploymentService;

            @Autowired
            private RuntimeDataService runtimeDataService;

            @Override
            public void run(String... strings) throws Exception {
                CustomIdKModuleDeploymentUnit unit = new CustomIdKModuleDeploymentUnit(deploymentId, "com.redhat.cajun.navy", "process-service", "1.0");

                unit.setStrategy(RuntimeStrategy.PER_REQUEST);

                KieContainer kieContainer = KieServices.Factory.get().newKieClasspathContainer();
                unit.setKieContainer(kieContainer);
                log.info("Service up and running");

                deploymentService.deploy(unit);

                Collection<ProcessDefinition> processes = runtimeDataService.getProcesses(new QueryContext());
                processes.forEach(p -> log.info(p.getName()));
            }
        };
    }
}
