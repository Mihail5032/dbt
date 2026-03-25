Apache Flink Dashboard
Overview
Jobs
Running Jobs
Completed Jobs
Task Managers
Job Manager
Submit New Job
Version: 2.1.0 Commit: 4cb6bd3 @ 2025-07-21T13:23:51+02:00 Message:
0

1

2

3

4

5

6

7

8

9

Uploaded Jars
Файл не выбран
Name	Upload Time	Entry Class	
original-xml-parser-correction-1.0.6.jar	2026-03-25, 18:04:48	
-
Delete
ru.x5.DataStreamJob
s3a://flink-operator-data/flink-checkpoints-dev/92700edf6c80d546dbcadd971fafbb2a/chk-413 audit - s3a://flink-operator-data/flink-checkpoints-dev/e637010257a03be730b14968c2fe9443/chk-42

Allow Non Restored State
xml-parser-audit-0.0.5.jar	2026-03-25, 18:03:41	
ru.x5.DataStreamJob
Delete

Server Response Message List

Plan Visualization
Server Response Message:
org.apache.flink.runtime.rest.handler.RestHandlerException: Could not execute application.
	at org.apache.flink.runtime.webmonitor.handlers.JarRunHandler.lambda$handleRequest$1(JarRunHandler.java:114)
	at java.base/java.util.concurrent.CompletableFuture.uniHandle(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture$UniHandle.tryFire(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture.postComplete(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(Unknown Source)
	at java.base/java.lang.Thread.run(Unknown Source)
Caused by: java.util.concurrent.CompletionException: org.apache.flink.util.FlinkRuntimeException: Could not execute application.
	at java.base/java.util.concurrent.CompletableFuture.encodeThrowable(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture.completeThrowable(Unknown Source)
	... 2 more
Caused by: org.apache.flink.util.FlinkRuntimeException: Could not execute application.
	at org.apache.flink.client.deployment.application.DetachedApplicationRunner.tryExecuteJobs(DetachedApplicationRunner.java:88)
	at org.apache.flink.client.deployment.application.DetachedApplicationRunner.run(DetachedApplicationRunner.java:70)
	at org.apache.flink.runtime.webmonitor.handlers.JarRunHandler.lambda$handleRequest$0(JarRunHandler.java:108)
	... 2 more
Caused by: org.apache.flink.client.program.ProgramInvocationException: The main method caused an error: Failed to execute job 'XML Parser with correction 11 transaction test'.
	at org.apache.flink.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:360)
	at org.apache.flink.client.program.PackagedProgram.invokeInteractiveModeForExecution(PackagedProgram.java:223)
	at org.apache.flink.client.ClientUtils.executeProgram(ClientUtils.java:105)
	at org.apache.flink.client.deployment.application.DetachedApplicationRunner.tryExecuteJobs(DetachedApplicationRunner.java:84)
	... 4 more
Caused by: org.apache.flink.util.FlinkException: Failed to execute job 'XML Parser with correction 11 transaction test'.
	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.executeAsync(StreamExecutionEnvironment.java:2008)
	at org.apache.flink.client.program.StreamContextEnvironment.executeAsync(StreamContextEnvironment.java:188)
	at org.apache.flink.client.program.StreamContextEnvironment.execute(StreamContextEnvironment.java:113)
	at org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.execute(StreamExecutionEnvironment.java:1846)
	at ru.x5.DataStreamJob.run(DataStreamJob.java:204)
	at ru.x5.DataStreamJob.main(DataStreamJob.java:37)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)
	at java.base/java.lang.reflect.Method.invoke(Unknown Source)
	at org.apache.flink.client.program.PackagedProgram.callMainMethod(PackagedProgram.java:343)
	... 7 more
Caused by: java.lang.RuntimeException: org.apache.flink.runtime.client.JobInitializationException: Could not start the JobMaster.
	at org.apache.flink.util.ExceptionUtils.rethrow(ExceptionUtils.java:321)
	at org.apache.flink.util.function.FunctionUtils.lambda$uncheckedFunction$2(FunctionUtils.java:75)
	at java.base/java.util.concurrent.CompletableFuture$UniApply.tryFire(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture$Completion.exec(Unknown Source)
	at java.base/java.util.concurrent.ForkJoinTask.doExec(Unknown Source)
	at java.base/java.util.concurrent.ForkJoinPool$WorkQueue.topLevelExec(Unknown Source)
	at java.base/java.util.concurrent.ForkJoinPool.scan(Unknown Source)
	at java.base/java.util.concurrent.ForkJoinPool.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ForkJoinWorkerThread.run(Unknown Source)
Caused by: org.apache.flink.runtime.client.JobInitializationException: Could not start the JobMaster.
	at org.apache.flink.runtime.jobmaster.DefaultJobMasterServiceProcess.lambda$new$0(DefaultJobMasterServiceProcess.java:97)
	at java.base/java.util.concurrent.CompletableFuture.uniWhenComplete(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture$UniWhenComplete.tryFire(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture.postComplete(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture$AsyncSupply.run(Unknown Source)
	at org.apache.flink.util.MdcUtils.lambda$wrapRunnable$1(MdcUtils.java:70)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at java.base/java.lang.Thread.run(Unknown Source)
Caused by: java.util.concurrent.CompletionException: java.lang.RuntimeException: java.io.FileNotFoundException: Cannot find checkpoint or savepoint file/directory 's3a://flink-operator-data/flink-checkpoints-dev/92700edf6c80d546dbcadd971fafbb2a/chk-413 audit - s3a://flink-operator-data/flink-checkpoints-dev/e637010257a03be730b14968c2fe9443/chk-42' on file system 's3a'.
	at java.base/java.util.concurrent.CompletableFuture.encodeThrowable(Unknown Source)
	at java.base/java.util.concurrent.CompletableFuture.completeThrowable(Unknown Source)
	... 5 more
Caused by: java.lang.RuntimeException: java.io.FileNotFoundException: Cannot find checkpoint or savepoint file/directory 's3a://flink-operator-data/flink-checkpoints-dev/92700edf6c80d546dbcadd971fafbb2a/chk-413 audit - s3a://flink-operator-data/flink-checkpoints-dev/e637010257a03be730b14968c2fe9443/chk-42' on file system 's3a'.
	at org.apache.flink.util.ExceptionUtils.rethrow(ExceptionUtils.java:321)
	at org.apache.flink.util.function.FunctionUtils.lambda$uncheckedSupplier$4(FunctionUtils.java:114)
	... 5 more
Caused by: java.io.FileNotFoundException: Cannot find checkpoint or savepoint file/directory 's3a://flink-operator-data/flink-checkpoints-dev/92700edf6c80d546dbcadd971fafbb2a/chk-413 audit - s3a://flink-operator-data/flink-checkpoints-dev/e637010257a03be730b14968c2fe9443/chk-42' on file system 's3a'.
	at org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(AbstractFsCheckpointStorageAccess.java:275)
	at org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.resolveCheckpoint(AbstractFsCheckpointStorageAccess.java:136)
	at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.restoreSavepoint(CheckpointCoordinator.java:1889)
	at org.apache.flink.runtime.scheduler.DefaultExecutionGraphFactory.tryRestoreExecutionGraphFromSavepoint(DefaultExecutionGraphFactory.java:217)
	at org.apache.flink.runtime.scheduler.DefaultExecutionGraphFactory.createAndRestoreExecutionGraph(DefaultExecutionGraphFactory.java:192)
	at org.apache.flink.runtime.scheduler.SchedulerBase.createAndRestoreExecutionGraph(SchedulerBase.java:402)
	at org.apache.flink.runtime.scheduler.SchedulerBase.(SchedulerBase.java:237)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.(DefaultScheduler.java:144)
	at org.apache.flink.runtime.scheduler.DefaultSchedulerFactory.createInstance(DefaultSchedulerFactory.java:173)
	at org.apache.flink.runtime.jobmaster.DefaultSlotPoolServiceSchedulerFactory.createScheduler(DefaultSlotPoolServiceSchedulerFactory.java:128)
	at org.apache.flink.runtime.jobmaster.JobMaster.createScheduler(JobMaster.java:407)
	at org.apache.flink.runtime.jobmaster.JobMaster.(JobMaster.java:384)
	at org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory.internalCreateJobMasterService(DefaultJobMasterServiceFactory.java:128)
	at org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory.lambda$createJobMasterService$0(DefaultJobMasterServiceFactory.java:100)
	at org.apache.flink.util.function.FunctionUtils.lambda$uncheckedSupplier$4(FunctionUtils.java:112)
	... 5 more
