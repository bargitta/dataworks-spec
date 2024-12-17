import json
import inspect
import traceback


class ListOfListsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, list):
            return obj
        return json.JSONEncoder.default(self, obj)


class SpecEntity(object):
    def __init__(self, metadata: dict = None):
        self.metadata = metadata


class SpecRefEntity(SpecEntity):
    def __init__(self, id: str = None):
        super(SpecRefEntity, self).__init__()
        self.id = id


class Strategy(object):
    def __init__(self, failureStrategy: str = None):
        self.failureStrategy = failureStrategy


class SpecScriptRuntime(SpecRefEntity):
    def __init__(self, engine: str = None,
                 command: str = None,
                 commandTypeId: int = None,
                 template: dict = None):
        super(SpecScriptRuntime, self).__init__()
        self.engine = engine
        self.command = command
        self.commandTypeId = commandTypeId
        self.template = template


class SpecVariable(SpecRefEntity):
    def __init__(self, name: str = None,
                 scope: str = None,
                 type: str = None,
                 value: str = None,
                 description: str = None):
        super(SpecVariable, self).__init__()
        self.name = name
        self.scope = scope
        self.type = type
        self.value = value
        self.description = description


class SpecScript(SpecRefEntity):
    def __init__(self,
                 path: str = None,
                 extension: str = None,
                 language: str = None,
                 runtime: SpecScriptRuntime = None,
                 parameters: list[SpecVariable] = None,
                 content: str = None):
        super(SpecScript, self).__init__()
        self.path = path
        self.extension = extension
        self.language = language
        self.runtime = runtime
        self.parameters = parameters
        self.content = content


class RuntimeResource(SpecRefEntity):
    def __init__(self, resourceGroup: str = None):
        super(RuntimeResource, self).__init__()
        self.resourceGroup = resourceGroup


class SpecTrigger(SpecRefEntity):
    def __init__(self,
                 id: str = None,
                 type: str = None,
                 cron: str = None,
                 startTime: str = None,
                 endTime: str = None,
                 timezone: str = None,
                 delaySeconds: int = None,
                 delay: int = None):
        super(SpecTrigger, self).__init__()
        self.id = id
        self.type = type
        self.cron = cron
        self.startTime = startTime
        self.endTime = endTime
        self.timezone = timezone
        self.delaySeconds = delaySeconds
        self.delay = delay


class SpecScheduleStrategy(SpecRefEntity):
    def __init__(self):
        super(SpecScheduleStrategy, self).__init__()
        self.priority: int
        self.timeout: int
        self.instanceMode: str
        self.rerunMode: str
        self.rerunTimes: int
        self.rerunInterval: int
        self.ignoreBranchConditionSkip: bool
        self.failureStrategy: str


class NodeOutput(SpecRefEntity):
    def __init__(self, data=None):
        super(NodeOutput, self).__init__()
        self.data = data


class SpecArtifact(SpecRefEntity):
    def __init__(self, artifactType: str = None):
        super(SpecArtifact, self).__init__()
        self.artifactType = artifactType


class Input(SpecRefEntity):
    def __init__(self):
        super(Input, self).__init__()


class Output(SpecRefEntity):
    def __init__(self):
        super(Output, self).__init__()


class SpecNodeIO(SpecArtifact):
    def __init__(self):
        super().__init__()
        self.data: str
        self.refTableName: str
        self.isDefault: bool


class NodeScript(SpecRefEntity):
    def __init__(self, runtime: SpecScriptRuntime = None):
        super(NodeScript, self).__init__()
        self.runtime = runtime


class SpecRuntimeResource(SpecRefEntity):
    def __init__(self,
                 resourceGroup: str = None,
                 resourceGroupId: str = None,
                 cu: str = None):
        super(SpecRuntimeResource, self).__init__()
        self.resourceGroup = resourceGroup
        self.resourceGroupId = resourceGroupId
        self.cu = cu


class SpecFileResource(SpecRefEntity):
    def __init__(self):
        super(SpecFileResource, self).__init__()
        self.name: str
        self.script: SpecScript
        self.runtimeResource: SpecRuntimeResource
        self.type: str
        self.datasource: SpecDatasource


class SpecDatasource(SpecRefEntity):
    def __init__(self,
                 name: str = None,
                 type: str = None,
                 subType: str = None,
                 config: str = None):
        super(SpecDatasource, self).__init__()
        self.name = name
        self.type = type
        self.subType = subType
        self.config = config


class SpecFile(SpecRefEntity):
    def __init__(self, path: str = None, extension: str = None):
        super(SpecFile, self).__init__()
        self.path = path
        self.extension = extension


class SpecComponentParameter(SpecRefEntity):
    def __init__(self):
        super(SpecComponentParameter, self).__init__()
        self.name: str
        self.type: str
        self.value: str
        self.args: str
        self.defaultValue: str
        self.description: str


class SpecComponent(SpecRefEntity):
    def __init__(self):
        super(SpecComponent, self).__init__()
        self.name: str
        self.owner: str
        self.description: str
        self.script: SpecScript
        self.inputs: list[SpecComponentParameter]
        self.outputs: {}


class SpecFunction(SpecRefEntity):
    def __init__(self):
        super(SpecFunction, self).__init__()
        self.name: str
        self.script: SpecScript
        self.type: str
        self.className: str
        self.datasource: SpecDatasource
        self.runtimeResource: SpecRuntimeResource
        self.fileResources: list[SpecFileResource]
        self.armResource: str
        self.usageDescription: str
        self.argumentsDescription: str
        self.returnValueDescription: str
        self.usageExample: str
        self.embeddedCodeType: str
        self.resourceType: str
        self.embeddedCode: str


class SpecBranch(SpecRefEntity):
    def __init__(self, when, desc, output):
        super(SpecBranch, self).__init__()
        self.when: str = when
        self.desc: str = desc
        self.output: SpecNodeIO = output


class SpecNode(SpecRefEntity):
    def __init__(self):
        super(SpecNode, self).__init__()
        self.recurrence: str
        self.priority: int
        self.timeout: int
        self.instanceMode: str
        self.rerunMode: str
        self.rerunTimes: int
        self.rerunInterval: int
        self.ignoreBranchConditionSkip: bool
        self.datasource: SpecDatasource
        self.script: NodeScript
        self.trigger: SpecTrigger
        self.runtimeResource: SpecRuntimeResource
        self.fileResources: list[SpecFileResource]
        self.functions: list[SpecFunction]
        self.inputs: list[Input]
        self.outputs: {}
        self.name: str
        self.owner: str
        self.description: str
        self.component: SpecComponent
        self.branch: list[SpecBranch]


class Depend(object):
    def __init__(self, type: str = None,
                 output: str = None):
        self.type = type
        self.output = output


class SpecNodeOutput(SpecRefEntity):
    def __init__(self, data: str = None, refTableName: str = None, isDefault: bool = None):
        super(SpecNodeOutput, self).__init__()
        self.data = data
        self.refTableName = refTableName
        self.isDefault = isDefault


class SpecDepend(SpecRefEntity):
    def __init__(self, nodeId: SpecNode = None, type: str = None, output: SpecNodeOutput = None):
        super(SpecDepend, self).__init__()
        self.nodeId = nodeId
        self.type = type
        self.output = output


class Flow(SpecRefEntity):
    def __init__(self,
                 nodeId: str = None,
                 depends: list[Depend] = []
                 ):
        super().__init__()
        self.nodeId = nodeId
        self.depends = depends


class SpecFlowDepend(SpecRefEntity):
    def __init__(self, nodeId: SpecNode = None, depends: list[SpecDepend] = None):
        super(SpecFlowDepend, self).__init__()
        self.nodeId = nodeId
        self.depends = depends


class Spec(object):
    def __init__(self):
        super(Spec, self).__init__()


class SpecWorkflow(SpecRefEntity):
    def __init__(self):
        super(SpecWorkflow, self).__init__()
        self.trigger: SpecTrigger
        self.inputs: list[Input]
        self.outputs: dict = {}
        self.strategy: SpecScheduleStrategy
        self.nodes: list[SpecNode] = []
        self.dependencies: list[SpecFlowDepend] = []
        self.name: str
        self.owner: str
        self.description: str
        script = SpecScript(runtime=SpecScriptRuntime(command="WORKFLOW"))
        self.script = script


class DataWorksWorkflowSpec(SpecRefEntity, Spec):
    def __init__(self):
        super(DataWorksWorkflowSpec, self).__init__()
        self.name: str
        self.type: str
        self.owner: str
        self.description: str
        self.variables: list[SpecVariable]
        self.triggers: list[SpecTrigger]
        self.scripts: list[SpecScript]
        self.files: list[SpecFile]
        self.artifacts: list[SpecArtifact]
        self.datasources: list[SpecDatasource]
        self.runtimeResources: list[SpecRuntimeResource]
        self.fileResources: list[SpecFileResource]
        self.functions: list[SpecFunction]
        self.nodes: list[SpecNode] = []
        self.workflows: list[SpecWorkflow] = []
        self.components: list[SpecComponent]
        self.flow: list[SpecFlowDepend] = []


class Specification(object):
    def __init__(self,
                 spec: Spec = None,
                 version: str = '1.1.0',
                 kind: str = 'CycleWorkflow'):
        self.version = version
        self.kind = kind
        self.spec = spec


from .dw_base import ExtendJSONEncoder


class SpecEntityEncoder(ExtendJSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, SpecEntity):
                return {k: v for k, v in obj.__dict__.items() if v is not None}
            elif isinstance(obj, SpecWorkflow) or isinstance(obj, DataWorksWorkflowSpec):
                return {k: v for k, v in obj.__dict__.items() if v is not None}
            elif isinstance(obj, Specification):
                return {k: v for k, v in obj.__dict__.items() if v is not None}
            elif isinstance(obj, str):
                return obj
            elif obj is None:
                return ''
            elif callable(obj):
                return inspect.getsource(obj)
            else:
                ExtendJSONEncoder.default(self, obj)
        except TypeError as e:
            traceback.print_exc(e)
            return obj.__str__()
