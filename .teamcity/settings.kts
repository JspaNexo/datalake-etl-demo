import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.buildSteps.DockerCommandStep
import jetbrains.buildServer.configs.kotlin.buildSteps.dockerCommand
import jetbrains.buildServer.configs.kotlin.buildSteps.python
import jetbrains.buildServer.configs.kotlin.triggers.vcs

// Si el servidor utiliza otra version, conservar la version de su DSL exportado.
version = "2026.2"

project {
    buildType(DatalakeBuildPackDocker)
}

object DatalakeBuildPackDocker : BuildType({
    id("Build")
    name = "DataLake - Build Pack Docker"
    description = "Pruebas Python, build Docker, integracion aislada y publicacion opcional"
    artifactRules = "artifacts/integration/** => integration\nartifacts/release/** => release"

    params {
        param("env.PYTHON_EXECUTABLE", "python3.12")
        param("env.PYTHONPATH", "%teamcity.build.checkoutDir%/src")
        param("env.APP_IMAGE_BUILD_NUMBER", "datalake-etl-demo:%build.number%")
        param("env.CI_PROJECT_NAME", "datalake-ci-%build.counter%")
        param("env.CI_GIT_REVISION", "%build.vcs.number%")
        param("env.PUBLISH_IMAGE", "false")
    }

    vcs {
        root(DslContext.settingsRoot)
    }

    steps {
        python {
            id = "Python_Tests"
            name = "Python Tests"
            executionMode = BuildStep.ExecutionMode.RUN_ON_SUCCESS
            pythonVersion = customPython { executable = "%env.PYTHON_EXECUTABLE%" }
            command = unittest {
                scriptArguments = "discover -s tests -v"
                isTestReportingEnabled = true
            }
        }

        dockerCommand {
            id = "Docker_Build_ETL"
            name = "Docker Build ETL"
            executionMode = BuildStep.ExecutionMode.RUN_ON_SUCCESS
            commandType = build {
                source = file { path = "Dockerfile" }
                contextDir = "."
                namesAndTags = "%env.APP_IMAGE_BUILD_NUMBER%"
                platform = DockerCommandStep.ImagePlatform.Linux
            }
        }

        python {
            id = "Integration_Tests"
            name = "Integration Tests - Compose aislado"
            executionMode = BuildStep.ExecutionMode.RUN_ON_SUCCESS
            pythonVersion = customPython { executable = "%env.PYTHON_EXECUTABLE%" }
            command = file { filename = "scripts/ci/run_integration.py" }
        }

        dockerCommand {
            id = "Docker_Push_Staging"
            name = "Docker Push Staging"
            enabled = false // Activar despues de configurar el registry.
            executionMode = BuildStep.ExecutionMode.RUN_ON_SUCCESS
            conditions {
                equals("env.PUBLISH_IMAGE", "true")
                equals("teamcity.build.branch.is_default", "true")
            }
            commandType = push {
                namesAndTags = "%env.APP_IMAGE_BUILD_NUMBER%"
                removeImageAfterPush = false
            }
        }

        python {
            id = "Prepare_Release_Files"
            name = "Prepare Release Files"
            executionMode = BuildStep.ExecutionMode.RUN_ON_SUCCESS
            pythonVersion = customPython { executable = "%env.PYTHON_EXECUTABLE%" }
            command = file { filename = "scripts/ci/prepare_release.py" }
        }
    }

    triggers {
        vcs { branchFilter = "+:*" }
    }

    requirements {
        exists("docker.server.version")
    }
})
