package com.aliyun.dataworks.migrationx.transformer.flowspec.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.SpecContext;
import com.aliyun.dataworks.common.spec.domain.SpecEntity;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.enums.SpecEntityType;
import com.aliyun.dataworks.common.spec.domain.interfaces.Input;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.Container;
import com.aliyun.dataworks.common.spec.domain.ref.InputOutputWired;
import com.aliyun.dataworks.common.spec.domain.ref.ScriptWired;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-21
 */
@Data
@Accessors(chain = true)
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class SpecRefEntityWrapper extends SpecRefEntity implements Container, InputOutputWired, ScriptWired {

    private SpecRefEntity specRefEntity;

    private SpecEntityType type;

    public SpecRefEntityWrapper setSpecRefEntity(SpecRefEntity entity) {
        this.specRefEntity = entity;
        // TODO refactor this, let SpecRefEntity can return its type
        if (entity != null) {
            if (entity.getClass().equals(SpecNode.class)) {
                this.type = SpecEntityType.NODE;
            } else if (entity.getClass().equals(SpecWorkflow.class)) {
                this.type = SpecEntityType.WORKFLOW;
            } else {
                this.type = null;
            }
            // fill basic info
            this.setId(entity.getId());
            this.setMetadata(entity.getMetadata());
            this.setContext(entity.getContext());
            this.setIsRef(entity.getIsRef());
        }
        return this;
    }

    @Override
    public String getId() {
        return Optional.ofNullable(getSpecRefEntity()).map(SpecRefEntity::getId).orElse(null);
    }

    @Override
    public Boolean getIsRef() {
        return Optional.ofNullable(getSpecRefEntity()).map(SpecRefEntity::getIsRef).orElse(null);

    }

    @Override
    public Map<String, Object> getMetadata() {
        return Optional.ofNullable(getSpecRefEntity()).map(SpecEntity::getMetadata).orElse(null);

    }

    @Override
    public SpecContext getContext() {
        return Optional.ofNullable(getSpecRefEntity()).map(SpecEntity::getContext).orElse(null);
    }

    public SpecNode getNode() {
        return getSpecRefEntity(SpecNode.class, SpecEntityType.NODE);
    }

    public SpecWorkflow getWorkflow() {
        return getSpecRefEntity(SpecWorkflow.class, SpecEntityType.WORKFLOW);
    }

    public Container getAsContainer() {
        return getSpecRefEntity(Container.class);
    }

    public InputOutputWired getAsInputOutputWired() {
        return getSpecRefEntity(InputOutputWired.class);
    }

    public ScriptWired getAsScriptWired() {
        return getSpecRefEntity(ScriptWired.class);
    }

    private <T extends SpecRefEntity> T getSpecRefEntity(Class<T> clazz, SpecEntityType entityType) {
        if (type == null || !type.equals(entityType)) {
            return null;
        }
        return getSpecRefEntity(clazz);
    }

    private <T> T getSpecRefEntity(Class<T> clazz) {
        return Optional.ofNullable(specRefEntity).filter(clazz::isInstance).map(clazz::cast).orElse(null);
    }

    @Override
    public List<SpecNode> getInnerNodes() {
        return Optional.ofNullable(getAsContainer()).map(Container::getInnerNodes).orElse(Collections.emptyList());
    }

    @Override
    public List<SpecFlowDepend> getInnerDependencies() {
        return Optional.ofNullable(getAsContainer()).map(Container::getInnerDependencies).orElse(Collections.emptyList());
    }

    @Override
    public List<Input> getInputs() {
        return Optional.ofNullable(getAsInputOutputWired()).map(InputOutputWired::getInputs).orElse(Collections.emptyList());
    }

    @Override
    public List<Output> getOutputs() {
        return Optional.ofNullable(getAsInputOutputWired()).map(InputOutputWired::getOutputs).orElse(Collections.emptyList());
    }

    @Override
    public SpecScript getScript() {
        return Optional.ofNullable(getAsScriptWired()).map(ScriptWired::getScript).orElse(null);
    }
}
