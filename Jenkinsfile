
node('build-slave') {

    try {
        
       stage('Checkout'){
          checkout scm
       }

       stage('Build Assets'){
          sh "mvn clean install -DskipTests"
        }
       
       stage('Archving Artifact'){
           archiveArtifacts("target/sunbird-cloud-store-sdk-1.0.jar")
       }
    }

    catch (err) {
        throw err
    }
}
