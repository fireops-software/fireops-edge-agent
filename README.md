# FireOps Edge Agent
The FireOpsEdgeAgent is a local operator, that handles the installation, update, destroy of all components of the fireops-edge-software. An takes all instruction from online backend using a websocket connection.

## Prerequisite
FireOpsEdgeAgent is a docker manager, that requires docker under the hood.

## Usage
```
Usage of ./fireops-edge-agent-linux-amd64:
  -api string
        Api endpoint for fireops websocket connection (required)
  -apiKey string
        Api key for fireops api (required)
  -logLvl string
        OFF,FATAL,ERROR,WARNING,INFO,DEBUG,TRACE (default "INFO")
  -namespace string
        namespace(label) for docker resources (default "fireops")

```

## Request-Types
The Request-Types are based on a given Datastructure. This structure defines common fields, that are necessary for routing the requests. All Requests/Responses contains a generic `Body`, that is specified by the given `MsgType`. Each messge contains a MsgId to be able to map a given request to a corresponding response (because async network communication in between). The common Respone-Types on the other hand conatins an additional Error field, which may hold some error message, if there is one. 

### GetAgentVersion
A GetAgentVersion request has to set the `MsgType` field to `"GetAgentVersion"`. This call returns the current agent version.

**Request:**
```json
{
  "MsgId":"f43aa496-8b6c-4967-ae2b-b65bb8ff2f9d",
  "MsgType":"GetAgentVersion",
  "Body":{}
}
```

**Response:**
```json
{
  "MsgId": "f43aa496-8b6c-4967-ae2b-b65bb8ff2f9d",
  "MsgType": "GetAgentVersion",
  "Error": null,
  "Body": {
    "Version": "<AGENT_VERSION>"
  }
}
```

### GetContainerLogs
A GetContainerLogs request has to set the `MsgType` field to `"GetContainerLogs"`. This call returns the the logs for the requested container. The number of returned lines can be managed using the `Len` field in the request body.

**Request:**
```json
{
  "MsgId":"f43aa496-8b6c-4967-ae2b-b65bb8ff2f9d",
  "MsgType":"GetContainerLogs",
  "Body":{
    "ContainerId": "<CONTAINER_ID>",
    "Len": "<LENGTH>"
  }
}
```

**Response:**
```json
{
  "MsgId": "f43aa496-8b6c-4967-ae2b-b65bb8ff2f9d",
  "MsgType": "GetContainerLogs",
  "Error": null,
  "Body": [
    {
      "Stream": "<stdout | stderr>",
      "Message": "<SOME_MESSAGE>",
    },
    ...
  ]
}
```

### GetContainers
A Get request has to set the `MsgType` field to `"GetContainers"`. This call initiate a request for the current system state or the current deployment. On successs, the Response contains the state of the running Docker infrastructure of the device, where `fireops-edge-agent` runs on, otherwise the error field contains the error message.

**Request:**
```json
{
  "MsgId":"f43aa496-8b6c-4967-ae2b-b65bb8ff2f9d",
  "MsgType":"GetContainers",
  "Body":{}
}
```

**Response:**
```json
{
  "MsgId": "f43aa496-8b6c-4967-ae2b-b65bb8ff2f9d",
  "MsgType": "GetContainers",
  "Error": null,
  "Body": [
    {
      "Id": "b9da6452c156ed7dce532d2479b4c5700b3314389f8c5f3eb59755da9f3d1379",
      "Names": [
        "/fireops-edge-dashboard"
      ],
      "Image": "fireops/fireops-edge-dashboard:1.1.0",
      "ImageID": "sha256:f17af607342bd4477f0b280b4d5ccfd3fdbae5ba22fd9e2a2d4a4ac75bb9a655",
      "Command": "/app/fireops-dashboard",
      "Created": 1753026233,
      "Ports": [
        {
          "IP": "0.0.0.0",
          "PrivatePort": 80,
          "PublicPort": 80,
          "Type": "tcp"
        }
      ],
      "Labels": {
        "fireops": ""
      },
      "State": "running",
      "Status": "Up Less than a second",
      "HostConfig": {
        "NetworkMode": "bridge"
      },
      "NetworkSettings": {
        "Networks": {
          "bridge": {
            "IPAMConfig": null,
            "Links": null,
            "Aliases": null,
            "MacAddress": "56:5a:e3:8a:63:07",
            "DriverOpts": null,
            "GwPriority": 0,
            "NetworkID": "5cbf671c19724cd25dd410b09d5289fb7fb1b4c03054847f8072d29ea0b3eaf8",
            "EndpointID": "26d2fa6459c3797a89d315057b6f433e5d78ea9eb1b6ccad82f36d220b1253a1",
            "Gateway": "172.17.0.1",
            "IPAddress": "172.17.0.2",
            "IPPrefixLen": 16,
            "IPv6Gateway": "",
            "GlobalIPv6Address": "",
            "GlobalIPv6PrefixLen": 0,
            "DNSNames": null
          },
          "fireops": {
            "IPAMConfig": null,
            "Links": null,
            "Aliases": null,
            "MacAddress": "a6:37:43:55:03:ab",
            "DriverOpts": null,
            "GwPriority": 0,
            "NetworkID": "b9994baf4606b42152d39e00abea7f2279639e55c16321e5b638e5e5dc3a3800",
            "EndpointID": "ffb8b9b91fe176b3ffcdda80540ca57db9ed6e5443463253511937d004eaaf77",
            "Gateway": "172.18.0.1",
            "IPAddress": "172.18.0.2",
            "IPPrefixLen": 16,
            "IPv6Gateway": "",
            "GlobalIPv6Address": "",
            "GlobalIPv6PrefixLen": 0,
            "DNSNames": null
          }
        }
      },
      "Mounts": []
    }
  ]
}
```

### InstallContainers
InstallContainers triggers a complete installation or replacement of the specified containers in request body, using the given specification in the request body. As response the currently running infrastructure is returned on success (same as Get request), otherwise an the error field contains the error message.

>**IMPORTANT:** It also replace a current installation

**Request:**
```json
{
  "MsgId": "c405edd9-79a4-4886-8fa0-48e8105b48ec",
  "MsgType": "InstallContainers",
  "Body": [
    {
      "ServiceName": "fireops-edge-dashboard",
      "Image": "fireops/fireops-edge-dashboard:1.1.0",
      "PortForwards": {
        "80": 80
      },
      "Environment": {
        "FIREDEP_ADDR": "Buxdrihudi 12, 5555 Buxdrihudi",
        "FIREDEP_LOGO_URL": "https://some.image.link",
        "FIREDEP_MAX_TIME_TEXT_TO_SPEECH": "600",
        "FIREDEP_NAME": "FF-Buxdrihudi",
        "FIREOPS_BASE_URL": "https://einsatz.bfk-perg.at",
        "FIREOPS_TOKEN": "FANCY_TOKEN",
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_PW": "fireops",
        "RABBITMQ_USER": "fireops"
      }
    }
  ]
}
```
**Response:**
```json
{
  "MsgId": "c405edd9-79a4-4886-8fa0-48e8105b48ec",
  "MsgType": "InstallContainers",
  "Error": null,
  "Body": [
    {
      "Id": "b9da6452c156ed7dce532d2479b4c5700b3314389f8c5f3eb59755da9f3d1379",
      "Names": [
        "/fireops-edge-dashboard"
      ],
      "Image": "fireops/fireops-edge-dashboard:1.1.0",
      "ImageID": "sha256:f17af607342bd4477f0b280b4d5ccfd3fdbae5ba22fd9e2a2d4a4ac75bb9a655",
      "Command": "/app/fireops-dashboard",
      "Created": 1753026233,
      "Ports": [
        {
          "IP": "0.0.0.0",
          "PrivatePort": 80,
          "PublicPort": 80,
          "Type": "tcp"
        }
      ],
      "Labels": {
        "fireops": ""
      },
      "State": "running",
      "Status": "Up Less than a second",
      "HostConfig": {
        "NetworkMode": "bridge"
      },
      "NetworkSettings": {
        "Networks": {
          "bridge": {
            "IPAMConfig": null,
            "Links": null,
            "Aliases": null,
            "MacAddress": "56:5a:e3:8a:63:07",
            "DriverOpts": null,
            "GwPriority": 0,
            "NetworkID": "5cbf671c19724cd25dd410b09d5289fb7fb1b4c03054847f8072d29ea0b3eaf8",
            "EndpointID": "26d2fa6459c3797a89d315057b6f433e5d78ea9eb1b6ccad82f36d220b1253a1",
            "Gateway": "172.17.0.1",
            "IPAddress": "172.17.0.2",
            "IPPrefixLen": 16,
            "IPv6Gateway": "",
            "GlobalIPv6Address": "",
            "GlobalIPv6PrefixLen": 0,
            "DNSNames": null
          },
          "fireops": {
            "IPAMConfig": null,
            "Links": null,
            "Aliases": null,
            "MacAddress": "a6:37:43:55:03:ab",
            "DriverOpts": null,
            "GwPriority": 0,
            "NetworkID": "b9994baf4606b42152d39e00abea7f2279639e55c16321e5b638e5e5dc3a3800",
            "EndpointID": "ffb8b9b91fe176b3ffcdda80540ca57db9ed6e5443463253511937d004eaaf77",
            "Gateway": "172.18.0.1",
            "IPAddress": "172.18.0.2",
            "IPPrefixLen": 16,
            "IPv6Gateway": "",
            "GlobalIPv6Address": "",
            "GlobalIPv6PrefixLen": 0,
            "DNSNames": null
          }
        }
      },
      "Mounts": []
    }
  ]
}
```

### DestroyContainers
A DestroyContainers request destroys all resources, that correspondes to the fireops-edge-agend namespace (specified via -namespace at cli - default: fireops). The response contains an error message, if any error occured.

**Request:**
```json
{
  "MsgId": "f4969dcc-ba2f-4388-a10e-f5ad2045f871",
  "MsgType": "DestroyContainers",
  "Body": {}
}
```
**Response:**
```json
{
  "MsgId": "f4969dcc-ba2f-4388-a10e-f5ad2045f871",
  "MsgType": "DestroyContainers",
  "Error": null,
  "Body": {}
}
```
