# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from flask import (
    g, Blueprint, jsonify, request, url_for
)

import airflow.api
from airflow.api.common.experimental import delete_dag as delete
from airflow.api.common.experimental import pool as pool_api
from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.get_dag_runs import get_dag_runs
from airflow.api.common.experimental.get_task import get_task
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.api.common.experimental.get_code import get_code
from airflow.api.common.experimental.get_dag_run_state import get_dag_run_state
from airflow.exceptions import AirflowException
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.app import csrf
from airflow import models
from airflow.utils.db import create_session
import hashlib
import imp
import importlib
import os
import sys
from flask import Flask,render_template,request,redirect,url_for
from werkzeug.utils import secure_filename
import os
from airflow import configuration
from airflow.configuration import run_command
from airflow.utils.timeout import timeout
from hdfs import InsecureClient, HdfsError
from airflow.models import DagBag

login_required = airflow.login.login_required
current_user = airflow.login.current_user

_log = LoggingMixin().log

requires_authentication = airflow.api.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['POST'])
@requires_authentication
def trigger_dag(dag_id):
    """
    Trigger a new dag run for a Dag with an execution date of now unless
    specified in the data.
    """
    data = request.get_json(force=True)

    run_id = None
    if 'run_id' in data:
        run_id = data['run_id']

    conf = None
    if 'conf' in data:
        conf = data['conf']

    execution_date = None
    if 'execution_date' in data and data['execution_date'] is not None:
        execution_date = data['execution_date']

        # Convert string datetime into actual datetime
        try:
            execution_date = timezone.parse(execution_date)
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                    execution_date))
            _log.info(error_message)
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

    try:
        dr = trigger.trigger_dag(dag_id, run_id, conf, execution_date)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    if getattr(g, 'user', None):
        _log.info("User %s created %s", g.user, dr)

    response = jsonify(message="Created {}".format(dr))
    return response


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>', methods=['DELETE'])
@requires_authentication
def delete_dag(dag_id):
    """
    Delete all DB records related to the specified Dag.
    """
    try:
        count = delete.delete_dag(dag_id)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    return jsonify(message="Removed {} record(s)".format(count), count=count)


@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['GET'])
@requires_authentication
def dag_runs(dag_id):
    """
    Returns a list of Dag Runs for a specific DAG ID.
    :query param state: a query string parameter '?state=queued|running|success...'
    :param dag_id: String identifier of a DAG
    :return: List of DAG runs of a DAG with requested state,
    or all runs if the state is not specified
    """
    try:
        state = request.args.get('state')
        dagruns = get_dag_runs(dag_id, state, run_url_route='airflow.graph')
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 400
        return response

    return jsonify(dagruns)


@api_experimental.route('/test', methods=['GET'])
@requires_authentication
def test():
    return jsonify(status='OK')


@api_experimental.route('/dags/<string:dag_id>/code', methods=['GET'])
@requires_authentication
def get_dag_code(dag_id):
    """Return python code of a given dag_id."""
    try:
        return get_code(dag_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """
    try:
        info = get_task(dag_id, task_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


# ToDo: Shouldn't this be a PUT method?
@api_experimental.route('/dags/<string:dag_id>/paused/<string:paused>', methods=['GET'])
@requires_authentication
def dag_paused(dag_id, paused):
    """(Un)pauses a dag"""

    DagModel = models.DagModel
    with create_session() as session:
        orm_dag = (
            session.query(DagModel)
                   .filter(DagModel.dag_id == dag_id).first()
        )
        if paused == 'true':
            orm_dag.is_paused = True
        else:
            orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

    return jsonify({'response': 'ok'})


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>/tasks/<string:task_id>',
    methods=['GET'])
@requires_authentication
def task_instance_info(dag_id, execution_date, task_id):
    """
    Returns a JSON with a task instance's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """

    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        info = get_task_instance(dag_id, task_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>',
    methods=['GET'])
@requires_authentication
def dag_run_status(dag_id, execution_date):
    """
    Returns a JSON with a dag_run's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """

    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        info = get_dag_run_state(dag_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    return jsonify(info)


@api_experimental.route('/latest_runs', methods=['GET'])
@requires_authentication
def latest_dag_runs():
    """Returns the latest DagRun for each DAG formatted for the UI. """
    from airflow.models import DagRun
    dagruns = DagRun.get_latest_runs()
    payload = []
    for dagrun in dagruns:
        if dagrun.execution_date:
            payload.append({
                'dag_id': dagrun.dag_id,
                'execution_date': dagrun.execution_date.isoformat(),
                'start_date': ((dagrun.start_date or '') and
                               dagrun.start_date.isoformat()),
                'dag_run_url': url_for('airflow.graph', dag_id=dagrun.dag_id,
                                       execution_date=dagrun.execution_date)
            })
    return jsonify(items=payload)  # old flask versions dont support jsonifying arrays


@api_experimental.route('/pools/<string:name>', methods=['GET'])
@requires_authentication
def get_pool(name):
    """Get pool by a given name."""
    try:
        pool = pool_api.get_pool(name=name)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@api_experimental.route('/pools', methods=['GET'])
@requires_authentication
def get_pools():
    """Get all pools."""
    try:
        pools = pool_api.get_pools()
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify([p.to_json() for p in pools])


@csrf.exempt
@api_experimental.route('/pools', methods=['POST'])
@requires_authentication
def create_pool():
    """Create a pool."""
    params = request.get_json(force=True)
    try:
        pool = pool_api.create_pool(**params)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@csrf.exempt
@api_experimental.route('/pools/<string:name>', methods=['DELETE'])
@requires_authentication
def delete_pool(name):
    """Delete pool."""
    try:
        pool = pool_api.delete_pool(name=name)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


def parse_dag_file(filepath):
    from airflow.models import DAG
    found_dags = []
    if filepath is None or not os.path.isfile(filepath):
        return found_dags
    mods = []
    org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
    mod_name = ('unusual_prefix_' +
                hashlib.sha1(filepath.encode('utf-8')).hexdigest() +
                '_' + org_mod_name)
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    with timeout(configuration.conf.getint('core', "DAGBAG_IMPORT_TIMEOUT")):
        try:
            m = imp.load_source(mod_name, filepath)
            mods.append(m)
        except Exception as e:
            _log.error(str(e))
            return found_dags
    for m in mods:
        for dag in list(m.__dict__.values()):
            if isinstance(dag, DAG):
                if not dag.full_filepath:
                    dag.full_filepath = filepath
                    if dag.fileloc != filepath:
                        dag.fileloc = filepath
                found_dags.append(dag)
    return found_dags


@csrf.exempt
@api_experimental.route('/upload_dag', methods=['POST'])
@login_required
def upload_dag():
    '''
    upload a dag file. a dag file can only contain one dag , and the dag name is
    the same as dag file name.
    '''
    if 'file' not in request.files:
        err = 'empty files in request'
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 500
        return response

    finfo = request.files['file']
    save_path = configuration.conf.get('webserver', 'upload_file_directory')
    if save_path is None:
        err = 'save directory not configured'
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 500
        return response

    file_path = os.path.join(save_path, secure_filename(finfo.filename))
    finfo.save(file_path)
    found_dags = parse_dag_file(file_path)
    if len(found_dags) == 0:
        err = "not found any dag in the file"
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 500
        return response
    elif len(found_dags) > 1:
        err = 'more than one dag in the dag file'
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 500
        return response
    dag = found_dags[0]
    if dag.dag_id + ".py" != finfo.filename:
        err = 'file name must be the same as dag id'
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 500
        return response

    if dag.default_args['owner'] != current_user.user.username and not current_user.is_superuser():
        err = 'invalid dag owner'
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 500
        return response
    dagbags = DagBag()
    if dag.dag_id in dagbags.dags:
        prev_dag = dagbags.get_dag(dag.dag_id)
        if prev_dag.default_args['owner'] != current_user.user.username and not current_user.is_superuser():

            err = 'you are not the dag owner'
            _log.error(err)
            response = jsonify(error="{}".format(err))
            response.status_code = 500
            return response

    webhdfs_url = configuration.conf.get('webserver', 'webhdfs_url')
    webhdfs_user = configuration.conf.get('webserver', 'webhdfs_user')
    hdfs_dags_path = configuration.conf.get('webserver', 'hdfs_dags_path')
    client = InsecureClient(webhdfs_url, user=webhdfs_user)
    client.upload(hdfs_path=hdfs_dags_path, local_path=file_path, overwrite=True,n_threads=1)

    os.remove(file_path)
    
    response = jsonify(error = 'upload OK')
    response.status_code = 200
    return response       

