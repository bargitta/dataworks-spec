package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.adf;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.*;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecSubFlow;
import com.aliyun.dataworks.common.spec.domain.ref.*;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.adf.AdfConf;
import com.aliyun.dataworks.migrationx.domain.adf.AdfPackage;
import com.aliyun.dataworks.migrationx.domain.adf.Pipeline;
import com.aliyun.dataworks.migrationx.domain.adf.Trigger;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class AdfConverter {
    private final AdfPackage adfPackage;
    private final AdfConf adfConf;

    public AdfConverter(AdfPackage adfPackage, AdfConf adfConf) {
        this.adfPackage = adfPackage;
        this.adfConf = adfConf;
    }

    public List<SpecWorkflow> convert() throws Exception {
        List<Pipeline> pipelines = adfPackage.getPipelines();
        Map<String, Trigger> triggers = adfPackage.getTriggers();
        if (CollectionUtils.isEmpty(pipelines)) {
            throw new RuntimeException("empty pipelines");
        }
        List<SpecWorkflow> flows = new ArrayList<>(pipelines.size());
        for (Pipeline pipeline : pipelines) {
            Trigger trigger = triggers.getOrDefault(pipeline.getName(), null);
            flows.add(toWorkflow(pipeline, trigger));
        }
        return flows;
    }

    private SpecWorkflow toWorkflow(Pipeline pipeline, Trigger trigger) throws NoSuchAlgorithmException {
        SpecWorkflow flow = new SpecWorkflow();
        String flowId = generateId(pipeline.getName());
        flow.setId(flowId);
        flow.setName(pipeline.getName());
        flow.setScript(getFlowSpecScript(pipeline));
        flow.setOutputs(getOutput(flowId, pipeline.getName())); // 本workflow的输出，供其他flow做依赖
        flow.setType(FlowType.CYCLE_WORKFLOW.getLabel());
        setTrigger(flow, trigger);
        setFlowNodesAndDependencies(flow, pipeline.getProperties().getActivities(), pipeline.getName());
        return flow;
    }

    private void setTrigger(SpecWorkflow flow, Trigger trigger) {
        if (trigger == null) {
            return;
        } else {
            SpecTrigger spec = new SpecTrigger();
            spec.setRecurrence(NodeRecurrenceType.NORMAL);

            flow.setTrigger(spec);
            if (trigger.getProperties() != null ) {
                if ("ScheduleTrigger".equalsIgnoreCase(trigger.getProperties().getType())) {
                    spec.setType(TriggerType.SCHEDULER);
                }
                Trigger.Recurrence recurrence = trigger.getProperties().getTypeProperties().getRecurrence();
                spec.setTimezone(recurrence.getTimeZone());
                spec.setStartTime(recurrence.getStartTime());
                spec.setCron(getCronFromConfig(trigger.getName()));
            }
        }
    }

    private String getCronFromConfig(String name) {
        Map<String, String> triggers = adfConf.getSettings().getTriggers();
        if(triggers.containsKey(name)){
            return triggers.get(name);
        }
        return null;
    }

    private void setFlowNodesAndDependencies(SpecWorkflow flow, List<Pipeline.PipelineProperty.Activity> activities, String pipelineName) throws NoSuchAlgorithmException {
        List<SpecNode> nodes = new ArrayList<>(activities.size());
        flow.setNodes(nodes);
        ArrayList<SpecFlowDepend> specDepends = new ArrayList<>();
        flow.setDependencies(specDepends);
        for (Pipeline.PipelineProperty.Activity activity : activities) {
            SpecNode node = new SpecNode();
            nodes.add(node);
            node.setName(activity.getName());
            node.setId(generateId(pipelineName + activity.getName()));
            node.setDescription(activity.getDescription());

            node.setScript(getNodeSpecScript(activity));
            node.setSubflow(getSubflow(activity));
            if ("Inactive".equalsIgnoreCase(activity.getState())) {
                node.setRecurrence(NodeRecurrenceType.SKIP);
            } else {
                node.setRecurrence(NodeRecurrenceType.NORMAL);
            }
            if (activity.getPolicy() != null) {
                if (activity.getPolicy().getRetry() != null && activity.getPolicy().getRetry() > 0) {
                    node.setRerunMode(NodeRerunModeType.ALL_ALLOWED);
                } else {
                    node.setRerunMode(NodeRerunModeType.ALL_DENIED);
                }
                node.setRerunTimes(activity.getPolicy().getRetry());
                node.setTimeout(toTimeoutInHours(activity.getPolicy().getTimeout()));
            }
            node.setOutputs(getOutput(node.getId(), node.getName()));
            node.setInstanceMode(NodeInstanceModeType.T_PLUS_1);

            List<Pipeline.PipelineProperty.Activity.DependActivity> dependsOn = activity.getDependsOn();
            if (CollectionUtils.isNotEmpty(dependsOn)) {
                SpecFlowDepend specDepend = new SpecFlowDepend();
                specDepend.setNodeId(node);
                specDepend.setDepends(new ArrayList<>(dependsOn.size()));
                specDepends.add(specDepend);

                for (Pipeline.PipelineProperty.Activity.DependActivity dependActivity : dependsOn) {
                    SpecDepend depend = new SpecDepend();
                    String id = generateId(pipelineName + dependActivity.getActivity());
                    SpecNode specNode = new SpecNode();
                    specNode.setId(id);
                    depend.setType(DependencyType.NORMAL);
                    SpecNodeOutput output = new SpecNodeOutput();
                    output.setData(id);
                    output.setRefTableName(dependActivity.getActivity());
                    depend.setOutput(output);
                    specDepend.getDepends().add(depend);
                }
            }
        }
    }

    private SpecSubFlow getSubflow(Pipeline.PipelineProperty.Activity activity) throws NoSuchAlgorithmException {
        if("ExecutePipeline".equalsIgnoreCase(activity.getType())){
            SpecSubFlow subFlow = new SpecSubFlow();
            String referencePipeline = activity.getTypeProperties().getPipeline().getReferenceName();
            String id = generateId(referencePipeline);
            subFlow.setOutput(id);
            return subFlow;
        }
        return null;
    }

    @NotNull
    private static SpecScript getNodeSpecScript(Pipeline.PipelineProperty.Activity activity) {
        SpecScript script = new SpecScript();
        script.setPath(activity.getName());
        script.setContent(getNodeContent(activity));
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        CodeProgramType command = getCommand(activity.getType());
        runtime.setCommand(command.getName());
        runtime.setCommandTypeId(command.getCode());
        script.setRuntime(runtime);
        return script;
    }

    private static String getNodeContent(Pipeline.PipelineProperty.Activity activity) {
        String activityType = activity.getType();
        if ("DatabricksNotebook".equalsIgnoreCase(activityType)) {
            return activity.getTypeProperties().getNotebookPath();
        } else if ("WebActivity".equalsIgnoreCase(activityType)) {
            return activity.getTypeProperties().getMethod() + " " + activity.getTypeProperties().getUrl();
        } else if("ExecutePipeline".equalsIgnoreCase(activityType)){
            return null;
        }
        throw new RuntimeException("not supported activity type " + activityType);
    }

    @NotNull
    private static CodeProgramType getCommand(String activityType) {
        if ("DatabricksNotebook".equalsIgnoreCase(activityType)) {
            return CodeProgramType.ODPS_SQL;
        } else if ("WebActivity".equalsIgnoreCase(activityType)) {
            return CodeProgramType.DIDE_SHELL;
        } else if("ExecutePipeline".equalsIgnoreCase(activityType)) {
            return CodeProgramType.SUB_PROCESS;
        }
        else {
            log.info("covert {} to dide_shell", activityType);
            return CodeProgramType.DIDE_SHELL;
        }
    }


    /**
     * The maximum time an activity can run with format D.HH:MM:SS, default value is 12 hours,min value is 10 minutes，
     * maximum value is 7 days。
     *
     * @param timeStr in D.HH:MM:SS
     * @return hours
     */
    public static int toTimeoutInHours(String timeStr) {
        if (timeStr == null || timeStr.trim().isEmpty()) {
            return 12;
        }

        String[] dayAndTime = timeStr.split("\\.");
        if (dayAndTime.length != 2) {
            log.info("Invalid format. Expected format D.HH:MM:SS {}", timeStr);
            return 12;
        }

        int days;
        try {
            days = Integer.parseInt(dayAndTime[0]);
        } catch (NumberFormatException e) {
            log.info("Invalid day value. {}", timeStr);
            return 12;
        }

        String timePart = dayAndTime[1];
        Duration duration;
        try {
            LocalTime lt = LocalTime.parse(timePart);
            duration = Duration.between(LocalTime.MIDNIGHT, lt);
        } catch (DateTimeParseException e) {
            log.info("Invalid time part. Expected format HH:MM:SS", e);
            return 12;
        }

        double totalHours = days * 24 + duration.getSeconds() / 3600.0;
        return (int) totalHours;
    }

    @NotNull
    private static SpecScript getFlowSpecScript(Pipeline pipeline) {
        SpecScript script = new SpecScript();
        if (pipeline.getProperties() != null && pipeline.getProperties().getFolder() != null) {
            String folderPath = pipeline.getProperties().getFolder().getName();
            script.setPath(folderPath + "/" + pipeline.getName());
        } else {
            script.setPath(pipeline.getName());
        }

        SpecScriptRuntime specScriptRuntime = new SpecScriptRuntime();
        specScriptRuntime.setCommand("WORKFLOW");
        script.setRuntime(specScriptRuntime);
        return script;
    }

    public List<Output> getOutput(String id, String name) {
        SpecNodeOutput output = new SpecNodeOutput();
        output.setData(id);
        output.setArtifactType(ArtifactType.NODE_OUTPUT);
        output.setRefTableName(name);
        return ImmutableList.of(output);
    }

    /**
     * generate id value from input string
     * @param input original id from adf in string format
     * @return a positive long/int value in string format
     * @throws NoSuchAlgorithmException when SHA-256 is missing
     */
    @NotNull
    private static String generateId(String input) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        ByteBuffer buffer = ByteBuffer.wrap(hashBytes);
        long longValue = buffer.getLong();
        if (longValue == Long.MIN_VALUE) {
            return String.valueOf(Math.abs(buffer.getInt()));
        }
        return String.valueOf(Math.abs(buffer.getLong()));
    }
}
