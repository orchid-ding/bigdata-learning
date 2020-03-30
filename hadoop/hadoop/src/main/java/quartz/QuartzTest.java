package quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;

import static org.quartz.JobBuilder.*;
  import static org.quartz.SimpleScheduleBuilder.*;

  public class QuartzTest {

      public static void main(String[] args) {

          try {
              // Grab the Scheduler instance from the Factory
              Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();


              // define the job and tie it to our HelloJob class
              JobDetail job = newJob(HelloJob.class)
                      .withIdentity("job1", "group1")
                      .build();
              // Trigger the job to run now, and then repeat every 40 seconds
              Trigger trigger = TriggerBuilder.newTrigger()
                      .withIdentity("trigger1", "group1")
                      .startAt(new Date("2019-02-11"))
                      .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                              .withIntervalInSeconds(40)
                              .withRepeatCount(0))
//                              .repeatForever())
                      .build();

              // Tell quartz to schedule the job using our trigger
              scheduler.scheduleJob(job, trigger);

              // and start it off
              scheduler.start();

          } catch (SchedulerException se) {
              se.printStackTrace();
          }
      }
  }