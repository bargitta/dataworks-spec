package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.subprocess;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode;
import com.aliyun.dataworks.common.spec.domain.interfaces.Input;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecJoin;
import com.aliyun.dataworks.common.spec.domain.ref.SpecArtifact;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.subprocess.SubProcessParameters;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.AbstractParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.migrationx.common.utils.BeanUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-25
 */
public abstract class BaseSubProcessParameterConverter extends AbstractParameterConverter<SubProcessParameters> {

    private final Map<String, String> convertIdMap = new HashMap<>();

    protected BaseSubProcessParameterConverter(DataWorksWorkflowSpec spec,
                                               SpecWorkflow specWorkflow,
                                               TaskDefinition taskDefinition,
                                               DolphinSchedulerV3ConverterContext context) {
        super(spec, specWorkflow, taskDefinition, context);
    }

    protected SpecWorkflow copyWorkflow(SpecWorkflow specWorkflow) {
        SpecWorkflow copyWorkflow = BeanUtils.deepCopy(specWorkflow, SpecWorkflow.class);
        copyWorkflow.setId(generateUuid(taskDefinition.getCode(), copyWorkflow));
        convertIdMap.put(specWorkflow.getId(), copyWorkflow.getId());

        // replace basic info
        copyWorkflow.setName(taskDefinition.getName());
        copyWorkflow.setDescription(taskDefinition.getDescription());
        copyWorkflow.setTrigger(this.getWorkflowTrigger());
        Optional.ofNullable(getWorkFlow()).map(SpecWorkflow::getStrategy).ifPresent(copyWorkflow::setStrategy);

        String workflowPath = getScriptPath(taskDefinition.getName());
        Optional.ofNullable(copyWorkflow.getScript()).ifPresent(script -> {
            script.setPath(workflowPath);
            Optional.ofNullable(getWorkFlow())
                .map(SpecWorkflow::getScript)
                .map(SpecScript::getRuntime)
                .ifPresent(script::setRuntime);
        });

        // replace output
        SpecNodeOutput defaultOutput = getDefaultOutput(copyWorkflow, false);
        Optional.ofNullable(defaultOutput).ifPresent(output -> {
            output.setId(generateUuid());
            output.setData(convertId(output.getData()));
        });

        // replace node ids, output ids and script param ids
        ListUtils.emptyIfNull(copyWorkflow.getInnerNodes())
            .forEach(node -> {
                String preId = node.getId();
                node.setId(generateUuid(node));
                convertIdMap.put(preId, node.getId());

                ListUtils.emptyIfNull(node.getOutputs()).stream()
                    .filter(output -> output instanceof SpecArtifact)
                    .map(output -> (SpecArtifact)output)
                    .forEach(output -> {
                        if (StringUtils.isNotBlank(output.getId())) {
                            String newUuid = generateUuid();
                            convertIdMap.put(output.getId(), newUuid);
                            output.setId(newUuid);
                        }
                    });
                Optional.ofNullable(node.getScript())
                    .map(SpecScript::getParameters)
                    .orElse(Collections.emptyList())
                    .forEach(v -> {
                        if (StringUtils.isNotBlank(v.getId())) {
                            String newUuid = generateUuid();
                            convertIdMap.put(v.getId(), newUuid);
                            v.setId(newUuid);
                        }
                    });

                node.setTrigger(copyWorkflow.getTrigger());

                String copyNodePath = FilenameUtils.concat(workflowPath, node.getName());
                Optional.ofNullable(node.getScript())
                    .ifPresent(script -> script.setPath(copyNodePath));
            });

        // reset the ids of various entities in nodes
        ListUtils.emptyIfNull(copyWorkflow.getInnerNodes()).forEach(this::resetNode);

        // replace dependencies
        // copy the dependencies with new id and reset id and data with converted id
        ListUtils.emptyIfNull(copyWorkflow.getInnerDependencies())
            .forEach(specFlowDepend -> {
                Optional.of(specFlowDepend)
                    .map(SpecFlowDepend::getNodeId)
                    .ifPresent(this::resetNode);
                ListUtils.emptyIfNull(specFlowDepend.getDepends())
                    .forEach(dep -> {
                        Optional.ofNullable(dep.getNodeId()).ifPresent(this::resetNode);
                        Optional.ofNullable(dep.getOutput()).ifPresent(this::resetOutput);
                    });
            });

        return copyWorkflow;
    }

    private void resetNode(SpecNode node) {
        if (node == null) {
            return;
        }
        node.setId(convertId(node.getId()));
        Optional.ofNullable(node.getScript())
            .map(SpecScript::getParameters)
            .orElse(Collections.emptyList())
            .forEach(v -> {
                if (StringUtils.isNotBlank(v.getId())) {
                    v.setId(convertId(v.getId()));
                }
            });
        ListUtils.emptyIfNull(node.getInputs()).forEach(this::resetInput);
        ListUtils.emptyIfNull(node.getOutputs()).forEach(this::resetOutput);

        if (node.getJoin() != null) {
            resetJoin(node.getJoin());
            ControllerJoinCode code = buildControllerJoinCode(node);
            Optional.ofNullable(node.getScript())
                .ifPresent(script -> script.setContent(code.getContent()));
        }
    }

    private void resetJoin(SpecJoin join) {
        if (join == null) {
            return;
        }
        ListUtils.emptyIfNull(join.getBranches())
            .forEach(branch -> {
                resetNode(branch.getNodeId());
                resetOutput(branch.getOutput());
            });
    }

    private void resetOutput(Output output) {
        if (output instanceof SpecArtifact) {
            ((SpecArtifact)output).setId(convertId(((SpecArtifact)output).getId()));
            if (output instanceof SpecNodeOutput) {
                ((SpecNodeOutput)output).setData(convertId(((SpecNodeOutput)output).getData()));
            }
        }
    }

    private void resetInput(Input input) {
        if (input instanceof SpecArtifact) {
            ((SpecArtifact)input).setId(convertId(((SpecArtifact)input).getId()));
            if (input instanceof SpecNodeOutput) {
                ((SpecNodeOutput)input).setData(convertId(((SpecNodeOutput)input).getData()));
            }
        }
    }

    private String convertId(String id) {
        return Optional.ofNullable(convertIdMap).map(map -> map.get(id)).orElse(id);
    }
}
