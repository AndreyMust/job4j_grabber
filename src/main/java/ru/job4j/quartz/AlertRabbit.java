package ru.job4j.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

public class AlertRabbit {

    public static Properties loadConfig() {
        Properties properties = new Properties();
        InputStream is = AlertRabbit.class.getClassLoader().getResourceAsStream("rabbit.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    public static void main(String[] args) {

        int interval = Integer.parseInt(loadConfig().getProperty("rabbit.interval"));

        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();

            /*Создание задачи*/
            JobDetail job = newJob(Rabbit.class).build();

            /*Создание расписания*/
            SimpleScheduleBuilder times = simpleSchedule()
                    .withIntervalInSeconds(interval)
                    .repeatForever();

            /*Задача выполняется через триггер*/
            Trigger trigger = newTrigger()
                    .startNow()
                    .withSchedule(times)
                    .build();
            /*Загрузка задачи в планировщик и подключение триггера */
            scheduler.scheduleJob(job, trigger);

        } catch (SchedulerException se) {
            se.printStackTrace();
        }
    }

    /*Реализация задачи для выполнения*/
    public static class Rabbit implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            System.out.println("Rabbit runs here ...");
        }
    }
}
