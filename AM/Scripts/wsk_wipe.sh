cd /home/$USER/openwhisk/ansible
ansible-playbook openwhisk.yml -e mode=clean
ansible-playbook routemgmt.yml -e mode=clean
ansible-playbook apigateway.yml -e mode=clean
ansible-playbook postdeploy.yml -e mode=clean
ansible-playbook wipe.yml
ansible-playbook couchdb.yml -e mode=clean
ansible-playbook setup.yml -e mode=clean
ansible-playbook invoker.yml -e mode=clean