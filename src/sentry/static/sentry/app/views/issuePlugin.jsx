import React from 'react';
import AlertActions from '../actions/alertActions';
import ApiMixin from '../mixins/apiMixin';
import {BooleanField, Form, Select2Field, TextareaField, TextField} from '../components/forms';
import GroupActions from '../actions/groupActions';
import GroupState from '../mixins/groupState';
import LoadingError from '../components/loadingError';
import LoadingIndicator from '../components/loadingIndicator';
import {t} from '../locale';

const IssuePlugin = React.createClass({
  propTypes: {
    plugin: React.PropTypes.object.isRequired
  },

  mixins: [
    ApiMixin,
    GroupState
  ],

  getInitialState() {
    return {
      createFieldList: null,
      linkFieldList: null,
      createIssue: true,
      loading: true,
      error: false,
      errorDetails: null,
      createFormData: {},
      linkFormData: {}
    };
  },

  componentWillMount() {
    let plugin = this.props.plugin;
    if (!plugin.issue) {
      this.fetchData();
    }
  },

  getPluginCreateEndpoint() {
    return ('/issues/' + this.getGroup().id +
            '/plugin/create/' + this.props.plugin.slug + '/');
  },

  getPluginLinkEndpoint() {
    return ('/issues/' + this.getGroup().id +
            '/plugin/link/' + this.props.plugin.slug + '/');
  },

  getPluginUnlinkEndpoint() {
    return ('/issues/' + this.getGroup().id +
            '/plugin/unlink/' + this.props.plugin.slug + '/');
  },

  fetchData() {
    this.setState({
      loading: true,
      error: false
    });

    this.api.request(this.getPluginCreateEndpoint(), {
      success: (data) => {
        let createFormData = {};
        data.forEach((field) => {
          createFormData[field.name] = field.default;
        });
        this.setState({
          createFieldList: data,
          error: false,
          loading: false,
          createFormData: createFormData
        });
      },
      error: (error) => {
        let state = {
          error: true,
          loading: false
        };
        if (error.status === 400 && error.responseJSON) {
          state.errorDetails = error.responseJSON;
        }
        this.setState(state);
      }
    });

    this.api.request(this.getPluginLinkEndpoint(), {
      success: (data) => {
        let linkFormData = {};
        data.forEach((field) => {
          linkFormData[field.name] = field.default;
        });
        this.setState({
          linkFieldList: data,
          error: false,
          loading: false,
          linkFormData: linkFormData
        });
      },
      error: (error) => {
        let state = {
          error: true,
          loading: false
        };
        if (error.status === 400 && error.responseJSON) {
          state.errorDetails = error.responseJSON;
        }
        this.setState(state);
      }
    });
  },

  createIssue() {
    this.api.request(this.getPluginCreateEndpoint(), {
      data: this.state.createFormData,
      success: (data) => {
        GroupActions.groupPluginChange(this.getGroup().id);
        AlertActions.addAlert({
          message: t('Successfully created issue.'),
          type: 'success'
        });
      },
      error: (error) => {
        AlertActions.addAlert({
          message: t('There was an error creating the issue.'),
          type: 'error'
        });
      }
    });
  },

  linkIssue() {
    this.api.request(this.getPluginLinkEndpoint(), {
      data: this.state.linkFormData,
      success: (data) => {
        GroupActions.groupPluginChange(this.getGroup().id);
        AlertActions.addAlert({
          message: t('Successfully linked issue.'),
          type: 'success'
        });
      },
      error: (error) => {
        AlertActions.addAlert({
          message: t('There was an error linking the issue.'),
          type: 'error'
        });
      }
    });
  },

  unlinkIssue() {
    this.api.request(this.getPluginUnlinkEndpoint(), {
      success: (data) => {
        GroupActions.groupPluginChange(this.getGroup().id);
        AlertActions.addAlert({
          message: t('Successfully unlinked issue.'),
          type: 'success'
        });
        this.fetchData();
      },
      error: (error) => {
        AlertActions.addAlert({
          message: t('There was an error unlinking the issue.'),
          type: 'error'
        });
      }
    });
  },

  changeField(action, name, value) {
    let key = action + 'FormData';
    let formData = this.state[key];
    formData[name] = value;
    let state = {};
    state[key] = formData;
    this.setState(state);
  },

  renderField(action, field) {
    let el;
    let props = {
      value: this.state[action + 'FormData'][field.name],
      onChange: this.changeField.bind(this, action, field.name),
      label: field.label,
      name: field.name
    };
    switch (field.type) {
      case 'text':
        el = <TextField {...props} />;
        break;
      case 'textarea':
        el = <TextareaField {...props} />;
        break;
      case 'select':
        if (field.has_autocomplete) {
          props.url = ('/api/0/issues/' + this.getGroup().id +
                       '/plugin/autocomplete/' + this.props.plugin.slug);
          el = <Select2Field {...props} />;
        }
        break;
      default:
        el = null;
    }
    return el;
  },

  toggleIssueForm(value) {
    this.setState({createIssue: value});
  },

  renderForm() {
    return (
      <div>
        <div>
          <BooleanField label={t('Create new issue')}
                        name="is_create"
                        value={this.state.createIssue}
                        onChange={this.toggleIssueForm}/>
        </div>
        {this.state.createIssue ?
          <Form onSubmit={this.createIssue}>
            {this.state.createFieldList.map((field) => {
              return <div key={field.name}>{this.renderField('create', field)}</div>;
            })}
          </Form> :
          <Form onSubmit={this.linkIssue}>
            {this.state.linkFieldList.map((field) => {
              return <div key={field.name}>{this.renderField('link', field)}</div>;
            })}
          </Form>
        }
      </div>
    );
  },

  getPluginConfigureUrl() {
    let org = this.getOrganization();
    let project = this.getProject();
    let plugin = this.props.plugin;
    return '/' + org.slug + '/' + project.slug + '/settings/plugins/' + plugin.slug;
  },

  renderError() {
    let error = this.state.errorDetails;
    if (!error) {
      return null;
    }
    if (error.error_type === 'auth') {
      return (
        <div className="alert alert-block">
          <p>You still need to <a href={error.auth_url}>associate an identity</a>
           {' with ' + error.title + ' before you can create issues with this service.'}</p>
        </div>
      );
    } else if (error.error_type === 'config') {
      return (
        <div className="alert alert-block">
            {!error.has_auth_configured ?
                <div>
                  <p>{('Your server administrator will need to configure authentication with ')}
                  <strong>{error.auth_provider}</strong>{(' before you can use this plugin.')}</p>
                  <p>{('The following settings must be configured:')}</p>
                  <ul>{error.required_auth_settings.map((setting) => {
                    return <li><code>{setting}</code></li>;
                  })}</ul>
                </div>
              :
              <p>You still need to <a href={this.getPluginConfigureUrl()}>configure this plugin</a> before you can use it.</p>}
        </div>
      );
    }
    return <LoadingError/>;
  },

  render() {
    let plugin = this.props.plugin;
    if (this.state.errorDetails) {
      return this.renderError();
    }
    if (plugin.issue) {
      return (
        <div>
          <a href={plugin.issue.url} target="_blank">{plugin.issue.label}</a>
          {plugin.can_unlink &&
            <button className="btn btn-primary"
                    onClick={this.unlinkIssue}>{t('Unlink')}</button>}
        </div>);
    }
    if (!this.state.createFieldList || (plugin.can_link_existing && !this.state.linkFieldList)) {
      return <LoadingIndicator />;
    }
    return this.renderForm();
  }
});


const GroupIssuePlugins = React.createClass({
  mixins: [
    GroupState
  ],

  render() {
    let group = this.getGroup();

    if (!(group.pluginIssues && group.pluginIssues.length)) {
      window.location = ('/' + this.getOrganization().slug + '/' +
                         this.getProject().slug + '/issues/' + this.getGroup().id);
      return null;
    }

    return (
      <div>
        {group.pluginIssues.map((plugin) => {
          return <IssuePlugin plugin={plugin} key={plugin.slug} />;
        })}
      </div>
    );
  }
});

export default GroupIssuePlugins;
