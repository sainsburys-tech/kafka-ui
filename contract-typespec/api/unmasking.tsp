import "@typespec/openapi";
import "./models.tsp";
import "./responses.tsp";

namespace Api;

using TypeSpec.Http;
using OpenAPI;

@route("/api/clusters/{clusterName}/topics/{topicName}/unmask")
@tag("Unmasking")
interface UnmaskingApi {
  @post
  @route("")
  @operationId("sendUnmaskRequest")
  @summary("sendUnmaskRequest")
  sendUnmaskRequest(
    @path clusterName: string,
    @path topicName: string,
    @body message: UnmaskRequest,
  ): void | ApiNotFoundResponse | ApiBadRequestResponse;
}

model UnmaskRequest {
  justification?: string;
}
