package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFile;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFunction;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.component.SpecComponent;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.NodeContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.NodeIo;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.NodeUseType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.RerunMode;
import lombok.Data;
import org.apache.commons.io.FilenameUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-12-12
 */
@Data
public class SpecFunctionEntityAdapter implements DwNodeEntity {

    private SpecFunction specFunction;

    @Override
    public String getUuid() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getId)
            .orElse(null);
    }

    @Override
    public Long getBizId() {
        return null;
    }

    @Override
    public String getBizName() {
        return null;
    }

    @Override
    public String getResourceGroupName() {
        return getResourceGroup();
    }

    @Override
    public Long getResourceGroupId() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getRuntimeResource)
            .map(SpecRuntimeResource::getResourceGroupId)
            .map(Long::valueOf)
            .orElse(null);
    }

    @Override
    public String getName() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getName)
            .orElse(null);
    }

    @Override
    public String getType() {
        Optional<SpecScriptRuntime> scriptRuntime = Optional.ofNullable(specFunction)
            .map(SpecFunction::getScript)
            .map(SpecScript::getRuntime);
        return scriptRuntime.map(SpecScriptRuntime::getCommand)
            .orElseGet(() -> scriptRuntime.map(SpecScriptRuntime::getCommandTypeId)
                .map(CodeProgramType::getNodeTypeByCode)
                .map(CodeProgramType::getName)
                .orElse(null));
    }

    @Override
    public Integer getTypeId() {
        Optional<SpecScriptRuntime> scriptRuntime = Optional.ofNullable(specFunction)
            .map(SpecFunction::getScript)
            .map(SpecScript::getRuntime);
        return scriptRuntime.map(SpecScriptRuntime::getCommandTypeId)
            .orElseGet(() -> scriptRuntime.map(SpecScriptRuntime::getCommand)
                .map(CodeProgramType::getNodeTypeByName)
                .map(CodeProgramType::getCode)
                .orElse(null));
    }

    @Override
    public String getCronExpress() {
        return null;
    }

    @Override
    public Date getStartEffectDate() {
        return null;
    }

    @Override
    public Integer getIsAutoParse() {
        return 0;
    }

    @Override
    public Date getEndEffectDate() {
        return null;
    }

    @Override
    public String getResourceGroup() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getRuntimeResource)
            .map(SpecRuntimeResource::getResourceGroup)
            .orElse(null);
    }

    @Override
    public String getDiResourceGroup() {
        return null;
    }

    @Override
    public String getDiResourceGroupName() {
        return null;
    }

    @Override
    public String getCodeMode() {
        return null;
    }

    @Override
    public Boolean getStartRightNow() {
        return null;
    }

    @Override
    public RerunMode getRerunMode() {
        return null;
    }

    @Override
    public Integer getNodeType() {
        return DwNodeEntity.super.getNodeType();
    }

    @Override
    public Boolean getPauseSchedule() {
        return null;
    }

    @Override
    public NodeUseType getNodeUseType() {
        return null;
    }

    @Override
    public String getRef() {
        return null;
    }

    @Override
    public String getFolder() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getScript)
            .map(SpecFile::getPath)
            .map(FilenameUtils::getFullPathNoEndSeparator)
            .orElse(null);
    }

    @Override
    public Boolean getRoot() {
        return null;
    }

    @Override
    public String getConnection() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getDatasource)
            .map(SpecDatasource::getName)
            .orElse(null);
    }

    @Override
    public String getCode() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getScript)
            .map(SpecScript::getContent)
            .orElse(null);
    }

    @Override
    public String getParameter() {
        return null;
    }

    @Override
    public List<NodeContext> getInputContexts() {
        return Collections.emptyList();
    }

    @Override
    public List<NodeContext> getOutputContexts() {
        return Collections.emptyList();

    }

    @Override
    public List<NodeIo> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public List<NodeIo> getOutputs() {
        return Collections.emptyList();
    }

    @Override
    public List<DwNodeEntity> getInnerNodes() {
        return Collections.emptyList();
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public Integer getTaskRerunTime() {
        return null;
    }

    @Override
    public Integer getTaskRerunInterval() {
        return null;
    }

    @Override
    public Integer getDependentType() {
        return null;
    }

    @Override
    public Integer getCycleType() {
        return null;
    }

    @Override
    public Date getLastModifyTime() {
        return null;
    }

    @Override
    public String getLastModifyUser() {
        return null;
    }

    @Override
    public Integer getMultiInstCheckType() {
        return null;
    }

    @Override
    public Integer getPriority() {
        return null;
    }

    @Override
    public String getDependentDataNode() {
        return null;
    }

    @Override
    public String getOwner() {
        return null;
    }

    @Override
    public String getOwnerName() {
        return null;
    }

    @Override
    public String getExtraConfig() {
        return null;
    }

    @Override
    public String getExtraContent() {
        return null;
    }

    @Override
    public String getTtContent() {
        return null;
    }

    @Override
    public String getAdvanceSettings() {
        return null;
    }

    @Override
    public String getExtend() {
        return null;
    }

    @Override
    public SpecComponent getComponent() {
        return null;
    }

    @Override
    public Integer getStreamLaunchMode() {
        return DwNodeEntity.super.getStreamLaunchMode();
    }

    @Override
    public Boolean getIgnoreBranchConditionSkip() {
        return DwNodeEntity.super.getIgnoreBranchConditionSkip();
    }

    @Override
    public Long getParentId() {
        return DwNodeEntity.super.getParentId();
    }

    @Override
    public String getCu() {
        return Optional.ofNullable(specFunction)
            .map(SpecFunction::getScript)
            .map(SpecScript::getRuntime)
            .map(SpecScriptRuntime::getCu)
            .orElse(null);
    }
}
