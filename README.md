# FireOps Edge Agent
The FireOpsEdgeAgent is a local operator, that handles the installation, update, destroy of all components of the fireops-edge-software. An takes all instruction from online backend using a websocket connection.

## Prerequisite
FireOpsEdgeAgent is a docker manager, that requires docker under the hood.

## Usage
```
Usage of ./fireops-agent:
  -api string
        Api endpoint for fireops websocket connection (required)
  -logLvl string
        OFF,FATAL,ERROR,WARNING,INFO,DEBUG,TRACE (default "INFO")
  -namespace string
        namespace(label) for docker resources (default "fireops")
  -token string
        Api token for fireops api (required)
```
