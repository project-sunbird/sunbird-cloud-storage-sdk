node('build-slave') {
    try {
        ansiColor('xterm') {
            stage('Checkout') {
                cleanWs()
                checkout scm
            }
            
            stage('Build Assets'){
               sh "mvn clean install -DskipTests=true"
           }
            
            stage('Archive artifacts'){
                commit_hash = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                branch_name = sh(script: 'git name-rev --name-only HEAD | rev | cut -d "/" -f1| rev', returnStdout: true).trim()
                artifact_version = branch_name + "_" + commit_hash
                sh """
                        mkdir cloud_storage_sdk_artifacts
                        cp target/sunbird-cloud-store-sdk-1.0.jar cloud_storage_sdk_artifacts
                        zip -j cloud_storage_sdk_artifacts.zip:${artifact_version} cloud_storage_sdk_artifacts
                    """
                archiveArtifacts artifacts: "cloud_storage_sdk_artifacts.zip:${artifact_version}", fingerprint: true, onlyIfSuccessful: true
                sh """echo {\\"artifact_name\\" : \\"cloud_storage_sdk_artifacts.zip\\", \\"artifact_version\\" : \\"${artifact_version}\\", \\"node_name\\" : \\"${env.NODE_NAME}\\"} > metadata.json"""
                archiveArtifacts artifacts: 'metadata.json', onlyIfSuccessful: true
            }
        }
    }

    catch (err) {
        currentBuild.result = "FAILURE"
        throw err
    }

}
