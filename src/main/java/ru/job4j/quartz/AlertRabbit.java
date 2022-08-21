package ru.job4j.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

public class AlertRabbit {

    public static Properties loadConfig() {
        Properties properties = new Properties();
        try (InputStream is = AlertRabbit.class.getClassLoader().getResourceAsStream("rabbit.properties")) {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    private static Connection connection(Properties properties) {
        Connection connection = null;
            try {
                Class.forName(properties.getProperty("jdbc.driver"));
                String url = properties.getProperty("jdbc.url");
                String login = properties.getProperty("jdbc.username");
                String password = properties.getProperty("jdbc.password");
                connection = DriverManager.getConnection(url, login, password);

            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        return connection;
    }


    public static void main(String[] args) {

        Properties properties = loadConfig();
        int interval = Integer.parseInt(properties.getProperty("rabbit.interval"));

        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();

            /*Добавление параметров для Job */
            JobDataMap data = new JobDataMap();
            data.put("connection", connection(properties));

            /*Создание задачи*/
            JobDetail job = newJob(Rabbit.class)
                    .usingJobData(data)
                    .build();

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

            Thread.sleep(5000);
            scheduler.shutdown();
            System.out.println("END-END");

        } catch (SchedulerException | InterruptedException se) {
            se.printStackTrace();
        }
    }

    /*Реализация задачи для выполнения*/
    public static class Rabbit implements Job {

        public Rabbit() {
            System.out.println(hashCode());
        }

        @Override
        public void execute(JobExecutionContext context) {
            System.out.println("Rabbit runs here ...");
            System.out.println("exec--------------");
            Connection connection = (Connection) context.getJobDetail().getJobDataMap().get("connection");
            try (
                    PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO rabbit (created_date) VALUES (?);")) {
                preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                preparedStatement.executeUpdate();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

        }
    }

    public static void mainDemo(String[] args) throws SQLException {
        Properties properties = loadConfig();
        Connection connection = connection(properties);
        System.out.println(connection.getSchema());
        System.out.println(connection.getCatalog());

        try (var statement = connection.createStatement()) {
            var selection = statement.executeQuery("select * from rabbit");
        }
    }
}
