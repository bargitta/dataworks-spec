package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.subprocess;

import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-24
 */
public class SubProcessParameterWithoutContainerConverter extends BaseSubProcessParameterConverter {

    public SubProcessParameterWithoutContainerConverter(DataWorksWorkflowSpec spec,
                                                        SpecWorkflow specWorkflow,
                                                        TaskDefinition taskDefinition,
                                                        DolphinSchedulerV3ConverterContext context) {
        super(spec, specWorkflow, taskDefinition, context);
    }

    @Override
    public SpecNode convert() {
        long processDefinitionCode = parameter.getProcessDefinitionCode();
        String workflowUuid = context.getUuidFromCode(processDefinitionCode);
        SpecRefEntityWrapper specRefEntityWrapper = context.getSpecRefEntityMap().get(workflowUuid);
        SpecWorkflow specWorkflow = Optional.ofNullable(specRefEntityWrapper).map(SpecRefEntityWrapper::getWorkflow).orElse(null);
        SpecWorkflow copyWorkflow = copyWorkflow(specWorkflow);

        headList.add(newWrapper(copyWorkflow));
        tailList.add(newWrapper(copyWorkflow));

        // add to sub workflows, triggers additional processing flows
        context.getSubWorkflows().add(copyWorkflow);
        return null;
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        // do nothing, because sub process will convert to a workflow instead of a node
    }
}
