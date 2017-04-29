/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *   http://www.apache.org/licenses/LICENSE-2.0
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
**/

import React, {Component, PropTypes} from 'react';
import ReactDOM from 'react-dom';
import _ from 'lodash';
import {Tabs, Tab} from 'react-bootstrap';
import Utils from '../../../utils/Utils';
import TopologyUtils from '../../../utils/TopologyUtils';
import TopologyREST from '../../../rest/TopologyREST';
import Form from '../../../libs/form';
import StreamsSidebar from '../../../components/StreamSidebar';
import NotesForm from '../../../components/NotesForm';
import ClusterREST from '../../../rest/ClusterREST';
import FSReactToastr from '../../../components/FSReactToastr';
import {toastOpt} from '../../../utils/Constants';
import CommonNotification from '../../../utils/CommonNotification';
import {Scrollbars} from 'react-custom-scrollbars';

export default class SinkNodeForm extends Component {
  static propTypes = {
    nodeData: PropTypes.object.isRequired,
    configData: PropTypes.object.isRequired,
    editMode: PropTypes.bool.isRequired,
    nodeType: PropTypes.string.isRequired,
    topologyId: PropTypes.string.isRequired,
    versionId: PropTypes.number.isRequired,
    sourceNodes: PropTypes.array.isRequired
  };

  constructor(props) {
    super(props);
    this.sourceNodesId = [];
    this.sourceChildNodeType = [];
    this.tempStreamFieldArr = [];
    props.sourceNodes.map((node) => {
      this.sourceNodesId.push(node.nodeId);
    });
    this.state = {
      formData: {},
      inputStreamArr : [],
      streamObj: {},
      description: '',
      showRequired: true,
      activeTabKey: 1,
      uiSpecification: [],
      clusterArr: [],
      clusterName: '',
      fetchLoader: true
    };
    this.fetchNotifier().then(() => {
      this.fetchData();
    });
  }

  getChildContext() {
    return {ParentForm: this};
  }

  fetchData() {
    let {
      topologyId,
      versionId,
      nodeType,
      nodeData,
      sourceNodes,
      namespaceId
    } = this.props;
    const sourceParams = nodeData.parentType + '/' + nodeData.topologyComponentBundleId;
    let sourceNodeType = null,sourceNodePromiseArr= [];
    let promiseArr = [
      TopologyREST.getNode(topologyId, versionId, nodeType, nodeData.nodeId),
      TopologyREST.getAllNodes(topologyId, versionId, 'edges'),
      TopologyREST.getSourceComponentClusters(sourceParams, namespaceId)
    ];
    if (sourceNodes.length > 0) {
      _.map(sourceNodes, (sourceNode) => {
        sourceNodePromiseArr.push(TopologyREST.getNode(topologyId, versionId ,TopologyUtils.getNodeType(sourceNode.parentType) ,sourceNode.nodeId));
      });

      // sourceNodeType = TopologyUtils.getNodeType(sourceNodes[0].parentType);
      // promiseArr.push(TopologyREST.getNode(topologyId, versionId, sourceNodeType, sourceNodes[0].nodeId));

    }
    Promise.all(promiseArr).then(results => {
      let stateObj = {},
        tempArr = [];
      this.nodeData = results[0];
      if (results[1].entities) {
        let tempStreamArr = [];
        results[1].entities.map((edge) => {
          if (edge.toId === nodeData.nodeId && this.sourceNodesId.indexOf(edge.fromId) !== -1) {
            //TODO - Once we support multiple input streams, need to fix this.
            TopologyREST.getNode(topologyId, versionId, 'streams', edge.streamGroupings[0].streamId).then(streamResult => {
              tempStreamArr.push(streamResult);
              _.map(tempStreamArr, (stream) => {
                this.tempStreamFieldArr.push(_.flattenDeep(stream.fields));
              });
              this.setState({inputStreamArr: tempStreamArr, streamObj :tempStreamArr[0],streamObjArr : tempStreamArr.length > 1 ? tempStreamArr : []});
            });
          }
        });
      }
      if (results[2].responseMessage !== undefined) {
        this.setState({fetchLoader: false});
        FSReactToastr.error(
          <CommonNotification flag="error" content={results[2].responseMessage}/>, '', toastOpt);
      } else {
        const clusters = results[2];
        _.keys(clusters).map((x) => {
          _.keys(clusters[x]).map(k => {
            if (k === "cluster") {
              const obj = {
                fieldName: clusters[x][k].name + '@#$' + clusters[x][k].ambariImportUrl,
                uiName: clusters[x][k].name
              };
              tempArr.push(obj);
            }
          });
        });
        stateObj.clusterArr = clusters;
      }
      if (!_.isEmpty(stateObj.clusterArr) && _.keys(stateObj.clusterArr).length > 1) {
        stateObj.uiSpecification = this.pushClusterFields(tempArr);
      }
      stateObj.formData = this.nodeData.config.properties;
      stateObj.description = this.nodeData.description;
      stateObj.formData.nodeType = this.props.nodeData.parentType;
      stateObj.fetchLoader = false;
      this.setState(stateObj, () => {
        if (stateObj.formData.cluster !== undefined) {
          const keyName = this.getClusterKey(stateObj.formData.cluster);
          this.updateClusterFields(keyName);
        }
        if (_.keys(stateObj.clusterArr).length === 1) {
          stateObj.formData.cluster = _.keys(stateObj.clusterArr)[0];
          this.updateClusterFields(stateObj.formData.cluster);
        }
      });

      Promise.all(sourceNodePromiseArr).then(connectedNodes => {
        _.map(connectedNodes, (connectedNode) => {
          if(connectedNode.responseMessage !== undefined){
            FSReactToastr.error(
              <CommonNotification flag="error" content={connectedNode.responseMessage}/>, '', toastOpt);
          }
        });

        let sourcePromiseArr = [];
        _.map(connectedNodes, (connectedNode,index) => {
          // sourceChildNodeType are processor nodes inner child, window or rule
          let type = sourceNodes[index].currentType.toLowerCase();
          this.sourceChildNodeType[index] = type === 'window'
            ? 'windows'
            : (type === 'rule' || type === 'projection'
              ? 'rules'
              : 'branchrules');

          if (connectedNode.config.properties && connectedNode.config.properties.rules && connectedNode.config.properties.rules.length > 0) {
            connectedNode.config.properties.rules.map((id) => {
              sourcePromiseArr.push(TopologyREST.getNode(topologyId, versionId, this.sourceChildNodeType[index], id));
            });
          }
        });

        Promise.all(sourcePromiseArr).then(sourceResults => {
          this.allSourceChildNodeData = sourceResults;
        });
      });
      // if (sourceNodes.length > 0) {
      //   //Finding the source node and updating actions for rules/windows
      //   this.sourceNodeData = results[3];
      //   let sourcePromiseArr = [];
      //   // sourceChildNodeType are processor nodes inner child, window or rule
      //   let type = sourceNodes[0].currentType.toLowerCase();
      //   this.sourceChildNodeType = type === 'window'
      //     ? 'windows'
      //     : (type === 'rule' || type === 'projection'
      //       ? 'rules'
      //       : 'branchrules');
      //   if (this.sourceNodeData.config.properties && this.sourceNodeData.config.properties.rules && this.sourceNodeData.config.properties.rules.length > 0) {
      //     this.sourceNodeData.config.properties.rules.map((id) => {
      //       sourcePromiseArr.push(TopologyREST.getNode(topologyId, versionId, this.sourceChildNodeType, id));
      //     });
      //   }
      //   Promise.all(sourcePromiseArr).then(sourceResults => {
      //     this.allSourceChildNodeData = sourceResults;
      //   });
      // }
    });
  }

  fetchFields = () => {
    let obj = this.props.configData.topologyComponentUISpecification.fields;
    const clusterFlag = obj.findIndex(x => {
      return x.fieldName === 'clusters';
    });
    if (clusterFlag === -1) {
      const data = {
        "uiName": "Cluster Name",
        "fieldName": "clusters",
        "isOptional": false,
        "tooltip": "Cluster name to read data from",
        "type": "CustomEnumstring",
        "options": []
      };
      obj.unshift(data);
    }
    return obj;
  }

  pushClusterFields = (opt) => {
    const uiSpecification = this.fetchFields();
    const obj = uiSpecification.map(x => {
      if (x.fieldName === 'clusters') {
        x.options = opt;
      }
      return x;
    });
    return obj;
  }

  fetchNotifier = () => {
    return ClusterREST.getAllNotifier().then(notifier => {
      if (notifier.responseMessage !== undefined) {
        FSReactToastr.error(
          <CommonNotification flag="error" content={notifier.responseMessage}/>, '', toastOpt);
      } else {
        const obj = notifier.entities.filter(x => {
          return x.name.indexOf("email_notifier") !== -1;
        });

        let {configData} = this.props;
        const {topologyComponentUISpecification} = configData;
        let uiFields = topologyComponentUISpecification.fields || [];

        uiFields.map(x => {
          if (x.fieldName === "jarFileName") {
            x.defaultValue = obj[0].jarFileName;
            if (x.hint !== undefined && x.hint.indexOf('hidden') === -1) {
              x.hint = x.hint + ',hidden';
            } else {
              x.hint = "hidden";
            }
          }
        });
        this.setState({uiSpecification: uiFields});
      }
    }).catch(err => {
      FSReactToastr.error(
        <CommonNotification flag="error" content={err.message}/>, '', toastOpt);
    });
  }

  validateData() {
    let validDataFlag = false;
    if (!this.state.fetchLoader) {
      if (this.refs.Form.validate()) {
        validDataFlag = true;
        this.setState({activeTabKey: 1, showRequired: true});
      }
    }
    return validDataFlag;
  }

  handleSave(name) {
    let {topologyId, versionId, nodeType, nodeData} = this.props;
    const {uiSpecification,inputStreamArr} = this.state;
    let nodeId = this.nodeData.id;
    let data = this.refs.Form.state.FormData;
    delete data.nodeType;
    this.nodeData.config.properties = data;
    let oldName = this.nodeData.name;
    this.nodeData.name = name;
    this.nodeData.description = this.state.description;
    let promiseArr = [TopologyREST.updateNode(topologyId, versionId, nodeType, nodeId, {
      body: JSON.stringify(this.nodeData)
    })
    ];

    // Check hint schema is present in the uiSpecification fields
    let schemaName ='';
    uiSpecification.map((x) => {
      if(x.hint !== undefined && x.hint.indexOf('schema') !== -1){
        _.keys(data).map((k) => {
          if(x.fieldName === k && !_.isEmpty(data[k])){
            schemaName = data[k];
          }
        });
      }
    });
    // if the hint schema is present then create the schema by POST request
    if(schemaName){
      const schemaData = {
        schemaMetadata: {
          type: "avro",
          schemaGroup: 'Kafka',
          name: schemaName.indexOf(":v") === -1 ? schemaName+":v" : schemaName,
          description: "auto_description",
          compatibility: "BACKWARD"
        },
        schemaVersion: {
          description : 'auto_description',
          schemaText : JSON.stringify(_.uniqBy(_.flattenDeep(this.tempStreamFieldArr),'name'))
        }
      };
      promiseArr.push(TopologyREST.createSchema({body : JSON.stringify(schemaData)}));
    }
    if (this.allSourceChildNodeData && this.allSourceChildNodeData.length > 0) {
      this.allSourceChildNodeData.map((childData,index) => {
        let child = childData;
        let obj = child.actions.find((o) => {
          return o.outputStreams[0] == inputStreamArr[index].streamId && o.name === 'notifierAction';
        });
        if (obj) {
          if (nodeData.currentType.toLowerCase() == 'notification') {
            obj.outputFieldsAndDefaults = this.nodeData.config.properties.fieldValues || {};
            obj.notifierName = this.nodeData.config.properties.notifierName || '';
          }
          promiseArr.push(TopologyREST.updateNode(topologyId, versionId, this.sourceChildNodeType[index], child.id, {body: JSON.stringify(child)}));
        }
      });
    }
    return Promise.all(promiseArr);
  }

  onSelectTab = (eventKey) => {
    if (eventKey == 1) {
      this.setState({activeTabKey: 1, showRequired: true});
    } else if (eventKey == 2) {
      this.setState({activeTabKey: 2, showRequired: false});
    } else if (eventKey == 3) {
      this.setState({activeTabKey: 3});
    }
  }

  handleNotesChange(description) {
    this.setState({description: description});
  }

  populateClusterFields(val) {
    const tempObj = Object.assign({}, this.state.formData, {topic: ''});
    const keyName = this.getClusterKey(val.split('@#$')[1]);
    this.setState({
      clusterName: keyName,
      formData: tempObj
    }, () => {
      this.updateClusterFields();
    });
  }

  getClusterKey(url) {
    const {clusterArr} = this.state;
    let key = '';
    _.keys(clusterArr).map(x => {
      _.keys(clusterArr[x]).map(k => {
        if (clusterArr[x][k].ambariImportUrl === url) {
          key = x;
        }
      });
    });
    return key;
  }

  updateClusterFields(name) {
    const {clusterArr, clusterName, formData,uiSpecification} = this.state;
    let tempFormData = _.cloneDeep(formData);

    /*
      Utils.mergeFormDataFields method accept params
      name =  name of cluster
      clusterArr = clusterArr array
      tempFormData = formData is fields of form
      uiSpecification = fields shown on ui depends on there options

      This method is responsible for showing default value of form fields
      and prefetch the value if its already configure
    */
    const {obj,tempData} = Utils.mergeFormDataFields(name,clusterArr, clusterName, tempFormData,uiSpecification);

    this.setState({uiSpecification: obj, formData: tempData});
  }

  render() {
    let {
      formData,
      streamObj = {},
      streamObjArr = [],
      uiSpecification,
      fetchLoader
    } = this.state;
    let fields = Utils.genFields(uiSpecification, [], formData, _.uniqBy(_.flattenDeep(this.tempStreamFieldArr),'name'));
    const form = fetchLoader
      ? <div className="col-sm-12">
          <div className="loading-img text-center" style={{
            marginTop: "100px"
          }}>
            <img src="styles/img/start-loader.gif" alt="loading"/>
          </div>
        </div>
      : <div className="sink-modal-form">
        <Scrollbars autoHide renderThumbHorizontal={props => <div {...props} style={{
          display: "none"
        }}/>}>
          <Form ref="Form" readOnly={!this.props.editMode} showRequired={this.state.showRequired} FormData={formData} className="customFormClass" populateClusterFields={this.populateClusterFields.bind(this)}>
            {fields}
          </Form>
        </Scrollbars>
      </div>;
    const inputSidebar = <StreamsSidebar ref="StreamSidebar" streamObj={streamObj} inputStreamOptions={streamObjArr} streamKind="input"/>;
    return (
      <Tabs id="SinkForm" activeKey={this.state.activeTabKey} className="modal-tabs" onSelect={this.onSelectTab}>
        <Tab eventKey={1} title="REQUIRED">
          {inputSidebar}
          {form}
        </Tab>
        <Tab eventKey={2} title="OPTIONAL">
          {inputSidebar}
          {form}
        </Tab>
        <Tab eventKey={3} title="NOTES">
          <NotesForm ref="NotesForm" description={this.state.description} onChangeDescription={this.handleNotesChange.bind(this)}/>
        </Tab>
      </Tabs>
    );
  }
}

SinkNodeForm.childContextTypes = {
  ParentForm: React.PropTypes.object
};
