package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import com.alibaba.fastjson2.JSON;

import com.aliyun.dataworks.common.spec.domain.dw.codemodel.CodeModel;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.CodeModelFactory;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ComponentSqlCode;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ComponentSqlCode.ComponentInfo;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.SqlComponentCode;
import com.aliyun.dataworks.common.spec.domain.dw.nodemodel.DataWorksNodeAdapter;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.component.SpecComponent;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.NodeContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.NodeIo;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.File;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeCfg;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeInputOutput;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeInputOutputContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.NodeUseType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.RerunMode;
import lombok.Data;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-11-21
 */
@Data
public class FileDetailEntityAdapter implements DwNodeEntity {

    private File file;

    private FileNodeCfg fileNodeCfg;

    @Override
    public String getUuid() {
        return Optional.ofNullable(file).map(File::getFileId).map(String::valueOf).orElse(null);
    }

    @Override
    public Long getBizId() {
        return Optional.ofNullable(file).map(File::getBizId).orElse(null);
    }

    @Override
    public String getBizName() {
        return null;
    }

    @Override
    public String getResourceGroupName() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getResourceGroupIdentifier)
            .orElse(null);
    }

    @Override
    public Long getResourceGroupId() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getResgroupId)
            .orElse(null);
    }

    @Override
    public String getName() {
        return Optional.ofNullable(file).map(File::getFileName).orElse(null);
    }

    @Override
    public String getType() {
        return Optional.ofNullable(file)
            .map(File::getFileType)
            .map(CodeProgramType::getNodeTypeByCode)
            .map(CodeProgramType::getName)
            .orElse(null);
    }

    @Override
    public Integer getTypeId() {
        return Optional.ofNullable(file).map(File::getFileType).orElse(null);
    }

    @Override
    public String getCronExpress() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getCronExpress)
            .orElse(null);
    }

    @Override
    public Date getStartEffectDate() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getStartEffectDate)
            .orElse(null);
    }

    @Override
    public Integer getIsAutoParse() {
        return Optional.ofNullable(file)
            .map(File::getIsAutoParse)
            .orElseGet(() -> Optional.ofNullable(fileNodeCfg)
                .map(FileNodeCfg::getIsAutoParse)
                .orElse(null));
    }

    @Override
    public Date getEndEffectDate() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getEndEffectDate)
            .orElse(null);
    }

    @Override
    public String getResourceGroup() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getResourceGroupIdentifier)
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
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getStartRightNow)
            .orElse(null);
    }

    @Override
    public RerunMode getRerunMode() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getReRunAble)
            .map(code -> {
                try {
                    return RerunMode.getByValue(code);
                } catch (Exception e) {
                    return RerunMode.UNKNOWN;
                }
            })
            .orElse(RerunMode.UNKNOWN);
    }

    @Override
    public Integer getNodeType() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getNodeType)
            .orElse(null);
    }

    @Override
    public Boolean getPauseSchedule() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getIsStop)
            .map(isStop -> isStop == 1)
            .orElse(null);
    }

    @Override
    public @NotNull NodeUseType getNodeUseType() {
        return Optional.ofNullable(file)
            .map(File::getUseType)
            .map(code -> {
                try {
                    return NodeUseType.getNodeUseTypeByValue(code);
                } catch (Exception e) {
                    return null;
                }
            })
            .orElse(NodeUseType.SCHEDULED);
    }

    @Override
    public String getRef() {
        return Optional.ofNullable(file)
            .map(File::getReference)
            .orElse(null);
    }

    @Override
    public String getFolder() {
        return Optional.ofNullable(file)
            .map(File::getFileFolderPath)
            .orElse(null);
    }

    @Override
    public Boolean getRoot() {
        return null;
    }

    @Override
    public String getConnection() {
        return Optional.ofNullable(file)
            .map(File::getConnName)
            .orElse(null);
    }

    @Override
    public String getCode() {
        return Optional.ofNullable(file)
            .map(File::getContent)
            .orElse(null);
    }

    @Override
    public String getParameter() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getParaValue)
            .orElse(null);
    }

    @Override
    public List<NodeContext> getInputContexts() {
        if (!Optional.ofNullable(fileNodeCfg).map(FileNodeCfg::getInputContextList).isPresent()) {
            return null;
        }
        return fileNodeCfg.getInputContextList().stream()
            .filter(Objects::nonNull)
            .map(this::toNodeContext)
            .collect(Collectors.toList());
    }

    @Override
    public List<NodeContext> getOutputContexts() {
        if (!Optional.ofNullable(fileNodeCfg).map(FileNodeCfg::getOutputContextList).isPresent()) {
            return null;
        }
        return fileNodeCfg.getOutputContextList().stream()
            .filter(Objects::nonNull)
            .map(this::toNodeContext)
            .collect(Collectors.toList());
    }

    @Override
    public List<NodeIo> getInputs() {
        if (!Optional.ofNullable(fileNodeCfg).map(FileNodeCfg::getInputList).isPresent()) {
            return null;
        }
        return fileNodeCfg.getInputList().stream()
            .filter(Objects::nonNull)
            .map(this::toNodeIo)
            .collect(Collectors.toList());
    }

    @Override
    public List<NodeIo> getOutputs() {
        if (!Optional.ofNullable(fileNodeCfg).map(FileNodeCfg::getOutputList).isPresent()) {
            return null;
        }
        return fileNodeCfg.getOutputList().stream()
            .filter(Objects::nonNull)
            .map(this::toNodeIo)
            .collect(Collectors.toList());
    }

    @Override
    public List<DwNodeEntity> getInnerNodes() {
        return new ArrayList<>();
    }

    @Override
    public String getDescription() {
        return Optional.ofNullable(file)
            .map(File::getFileDesc)
            .orElse(null);
    }

    @Override
    public Integer getTaskRerunTime() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getTaskRerunTime)
            .orElse(null);
    }

    @Override
    public Integer getTaskRerunInterval() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getTaskRerunInterval)
            .map(Math::toIntExact)
            .orElse(null);
    }

    @Override
    public Integer getDependentType() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getDependentType)
            .orElse(null);
    }

    @Override
    public Integer getCycleType() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getCycleType)
            .orElse(null);
    }

    @Override
    public Date getLastModifyTime() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getLastModifyTime)
            .orElse(null);
    }

    @Override
    public String getLastModifyUser() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getLastModifyUser)
            .orElse(null);
    }

    @Override
    public Integer getMultiInstCheckType() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getMultiinstCheckType)
            .orElse(null);
    }

    @Override
    public Integer getPriority() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getPriority)
            .orElse(null);
    }

    @Override
    public String getDependentDataNode() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getDependentDataNode)
            .orElse(null);
    }

    @Override
    public String getOwner() {
        return Optional.ofNullable(file)
            .map(File::getOwner)
            .orElse(null);
    }

    @Override
    public String getOwnerName() {
        return Optional.ofNullable(file)
            .map(File::getOwnerName)
            .orElse(null);
    }

    @Override
    public String getExtraConfig() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getExtConfig)
            .orElse(null);
    }

    @Override
    public String getExtraContent() {
        return Optional.ofNullable(file)
            .map(File::getExtraContent)
            .orElse(null);
    }

    @Override
    public String getTtContent() {
        return Optional.ofNullable(file)
            .map(File::getTtContent)
            .orElse(null);
    }

    @Override
    public String getAdvanceSettings() {
        return Optional.ofNullable(file)
            .map(File::getAdvanceSettings)
            .orElse(null);
    }

    @Override
    public String getExtend() {
        return Optional.ofNullable(file)
            .map(File::getExtend)
            .orElse(null);
    }

    @Override
    public SpecComponent getComponent() {
        return Optional.ofNullable(CodeProgramType.getNodeTypeByName(getType()))
            .map(type -> {
                if (CodeProgramType.COMPONENT_SQL.name().equalsIgnoreCase(type.name())) {
                    return getComponentSql();
                } else if (CodeProgramType.SQL_COMPONENT.name().equalsIgnoreCase(type.name())) {
                    return getSqlComponent();
                }
                return null;
            })
            .orElse(null);
    }

    @Override
    public String getImageId() {
        return DwNodeEntity.super.getImageId();
    }

    @Override
    public Long getCalendarId() {
        return DwNodeEntity.super.getCalendarId();
    }

    @Override
    public Integer getStreamLaunchMode() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getStreamLaunchMode)
            .orElse(null);
    }

    @Override
    public Boolean getIgnoreBranchConditionSkip() {
        return Optional.ofNullable(fileNodeCfg)
            .map(FileNodeCfg::getExtConfig)
            .map(config -> {
                try {
                    return JSON.parseObject(config);
                } catch (Exception e) {
                    return null;
                }
            })
            .filter(config -> config.containsKey(DataWorksNodeAdapter.IGNORE_BRANCH_CONDITION_SKIP))
            .map(config -> config.getBoolean(DataWorksNodeAdapter.IGNORE_BRANCH_CONDITION_SKIP))
            .orElse(null);
    }

    @Override
    public Long getParentId() {
        return Optional.ofNullable(file)
            .map(File::getParentId)
            .orElse(null);
    }

    @Override
    public String getCu() {
        return DwNodeEntity.super.getCu();
    }

    private NodeContext toNodeContext(FileNodeInputOutputContext nodeInputOutputContext) {
        if (nodeInputOutputContext == null) {
            return null;
        }
        NodeContext nodeContext = new NodeContext();
        nodeContext.setType(nodeInputOutputContext.getType());
        nodeContext.setParamName(nodeInputOutputContext.getParamName());
        nodeContext.setParamValue(nodeInputOutputContext.getParamValue());
        nodeContext.setParamType(nodeInputOutputContext.getParamType());
        nodeContext.setParseType(nodeInputOutputContext.getParseType());
        nodeContext.setDescription(nodeInputOutputContext.getDescription());
        nodeContext.setEditable(nodeInputOutputContext.getEditable());
        nodeContext.setNodeId(nodeInputOutputContext.getNodeId());
        nodeContext.setParamNodeId(nodeInputOutputContext.getParamNodeId());
        nodeContext.setOutput(nodeInputOutputContext.getOutput());
        return nodeContext;
    }

    private NodeIo toNodeIo(FileNodeInputOutput nodeInputOutput) {
        if (nodeInputOutput == null) {
            return null;
        }
        NodeIo nodeIo = new NodeIo();
        nodeIo.setData(nodeInputOutput.getStr());
        nodeIo.setParseType(nodeInputOutput.getParseType());
        nodeIo.setRefTableName(nodeInputOutput.getRefTableName());
        return nodeIo;
    }

    private SpecComponent getSqlComponent() {
        CodeModel<SqlComponentCode> codeModel = CodeModelFactory.getCodeModel(CodeProgramType.SQL_COMPONENT.name(), getCode());
        SpecComponent com = Optional.ofNullable(codeModel.getCodeModel()).map(SqlComponentCode::getConfig).orElse(new SpecComponent());
        com.setId(getUuid());
        com.setName(getName());
        com.setOwner(getOwner());
        com.setDescription(getDescription());
        com.setInputs(com.getInputs());
        com.setOutputs(com.getOutputs());

        SpecScript script = new SpecScript();
        script.setContent(codeModel.getCodeModel().getCode());
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.SQL_COMPONENT.getName());
        runtime.setCommandTypeId(CodeProgramType.SQL_COMPONENT.getCode());
        script.setRuntime(runtime);
        com.setScript(script);
        return com;
    }

    private SpecComponent getComponentSql() {
        CodeModel<ComponentSqlCode> codeModel = CodeModelFactory.getCodeModel(CodeProgramType.COMPONENT_SQL.name(), getCode());
        SpecComponent com = Optional.ofNullable(codeModel.getCodeModel().getConfig()).orElse(new SpecComponent());
        Optional.ofNullable(codeModel.getCodeModel().getComponent()).map(ComponentInfo::getId).map(String::valueOf).ifPresent(com::setId);
        Optional.ofNullable(codeModel.getCodeModel().getComponent()).map(ComponentInfo::getName).ifPresent(com::setName);

        Map<String, Object> metadata = new HashMap<>();
        Optional.ofNullable(codeModel.getCodeModel())
            .map(ComponentSqlCode::getComponent)
            .map(ComponentInfo::getVersion)
            .ifPresent(version -> metadata.put("version", version));
        metadata.put("id", com.getId());
        com.setMetadata(metadata);
        return com;
    }
}
