# Snowplow Indicative Relay
[![Build Status][travis-image]][travis]
[![Release][release-image]][release]
[![License][license-image]][license]

Snowplow Indicative Relay is an AWS Lambda function that reads Snowplow enriched events
from a Kinesis Stream and transfers them to Indicative. It processes events in batches, which
size depends on your AWS Lambda configuration.


## Usage

### Code deployment

We host a jar file on S3 through Snowplow [hosted assets][hosted-assets].
For example:
```
s3://snowplow-hosted-assets/relays/indicative/indicative-relay-0.1.0.jar
```
You will need to use a S3 bucket in the same region as your lambda. The above URL is for the *eu-west-1* region.
Buckets in other regions have their region names added to the base of the URL, like this:
```
s3://snowplow-hosted-assets-us-east-1/relays/indicative/indicative-relay-0.1.0.jar
s3://snowplow-hosted-assets-eu-central-1/relays/indicative/indicative-relay-0.1.0.jar
```

### Creating the Lambda

Your AWS Lambda needs to have an Execution Role that allows it to use the Kinesis Stream.
To create one please follow [the official AWS tutorial][aws-tutorial].
Then create a lambda function with the created role, either through AWS Console or through the CLI.

In the *Handler* textbox paste `com.snowplowanalytics.indicative.LambdaHandler::recordHandler`

You will need to provide the Indicative API Key as an environment variable `INDICATIVE_API_KEY`.

Finally, add your Snowplow enriched Kinesis stream as an event source for the lambda function.


## Copyright and license

Snowplow is copyright 2018-2018 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License"); you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[travis-image]: https://travis-ci.org/snowplow-incubator/snowplow-indicative-relay.svg?branch=master
[travis]: https://travis-ci.org/snowplow-incubator/snowplow-indicative-relay

[release-image]: https://img.shields.io/badge/release-0.1.0-orange.svg?style=flat
[release]: https://github.com/snowplow-incubator/snowplow-indicative-relay/releases

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[hosted-assets]: https://github.com/snowplow/snowplow/wiki/Hosted-assets
[aws-tutorial]: https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis-example-create-iam-role.html
