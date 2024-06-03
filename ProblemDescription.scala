
/*
We have to implement a system that receives messages, process it, and then answer with a response. Does it sound familiar to you?
We decided to implemented it with Akka Streams so we will do it in a Reactive Programing style.
*/

// We have applyed the Single Responsability Principle and we have divided our messaging processing logic in smaller actions that can be easily tested in isolation.
// But then we should combine all pieces together to build up an implementation for: 

trait MessageProcessor{
  def process(in: Source[Request, NotUsed]): Source[Response, NotUsed]	
}


// So we have the following types regarding to messages  

case class Request(id: String)

trait Response
case class ResponseOk() extends Response
case class ResponseError() extends Response


//And here we have our business logic pieces:

trait Cache {
  def getFromCache(msg: Request): Future[Option[Response]]	
}

trait ParseMessage {
  def parse(msg: Request): Future[ParsedMessage]
}


trait BussinessLogic{
  def process(msg: ParsedMessage): Future[BussinessResult]
}

trait PublishResult{	
  def publish(msg: BussinessResult): Future[Unit]
}


trait ResponseBuilder {
  def buildResponse(result: BussinessResult): Future[Response]
}

/////////////////////////////////////////////////////////////////////////////
/*

We have a success flow where components will be executed one after another:
1- parse message, and then...
2- chech if message is in cache, if it's not then...
3- we apply bussiness logic and then ...
4- we publish the resutl and then...
5. we build a response

---> Stage1 ---> Stage 2 ---> ... ---> Stage n

But we have still a couple of things to refine in this chain of executions...

- In case of cache hit we don't have to execute any further stages.

Akka Streams DSL doesn't have a solution out of the box to deal with this but this could be solved by having two kind of result for a stage. 
One would be an intermediate result that would indicate that  we have to execute the following step,
or a final result that would indicate that we are done, that we already have the response message. 
That could happent because of an error or because of we have the response chached so we don't have to compute it again...

- What if we want to execute PublishResult asynchronously? 
  We will need to implement a way of async execution of stages...
	
To make it even better let's say that to generate response we need data from request message.

*/
/////////////////////////////////////////////////////////////////////////////

case class Context(original: Request, data: ContextData)

sealed trait Result[+A]
case class FinalResult(result: Response) extends Result[Nothing]
case class PartialResult[A](underlying: A, context: Context) extends Result[A]


def handleError(t: Throwable, request: Request): ResponseError


implicit class ResultCombinators[A](in: Future[Result[A]]) extends Tracing {
    def andNext[B](f: A => Future[B]): Future[Result[B]] =
      in.flatMap {
        case result: FinalResult => Future.successful(result)
        case PartialResult(value, context) =>
          f(value)
            .map(PartialResult(_, context))
            .recover(t => FinalResult(handleError(t, context.original)))
      }

    def andLast(f: A => Future[Response]): Future[FinalResult] =
      in.flatMap {
        case result: FinalResult => Future.successful(result)
        case PartialResult(value, context) =>
          f(value)
            .map(FinalResult)
            .recover(t => FinalResult(handleError(t, context.original)))
      }

    def andNextAsync(f: A => Future[Unit]): Future[Result[A]] = {
      in.flatMap {
        case result: FinalResult => Future.successful(result)
        case PartialResult(underlying, context) =>
          f(underlying)
          in
      }
    }

    def responseOrNext(f: A => Future[Option[Response]])(
        next: Future[Result[A]] => Future[FinalResult]
    ): Future[FinalResult] = in.flatMap {
      case result: FinalResult => Future.successful(result)
      case PartialResult(value, context) =>
        f(value).flatMap {
          case Some(value) => FinalResult(value)
          case None => next(in) 
        }
    }
  }

  implicit class EnvelopeOps(in: Envelope) extends Tracing {
    def withContext(): Future[PartialResult[Envelope]] =
      Future.successful(PartialResult(in, Context(in, createSpan)))
  }
  
  
/*

This would allow us to compose our flow like this:
*/

class EnvelopeProcessingFlow(
      cache: Cache,
      parser: ParseMessage,
      extractor: Extractor,
      processor: BussinessLogic,
      publisher: PublishResult
      responseBuilder: ResponseBuilder,    
      parallelism: Int  
  ) extends MessageProcessor(implicit
    val ec: ExecutionContext,
    val mat: Materializer
   ) {

    private def processMessage(in: Request): Future[Response] =
      in.withContext()
        .andNext(parser.parse)
        .responseOrNext(cache.getFromCache) { parsedMessage =>
          parsedMessage
            .andNext(validator.validate)
            .andNext(processor.process)
            .andNextAsync(publisher.publish)
            .andLast(responseBuilder.buildResponse)
        }
        .map(_.result)

   def process(in: Source[Request, NotUsed]): Source[Response, NotUsed] = 
   	in.mapAsyncUnordered(parallelism)(processMessage)
  }

// Flaws:

// 1- You can observe that, in general case, if the first element of the chain is returning a FinalResult, 
// this value goes through all the stages of processing being wrapped and unwrapped from Future until 
// andLast is called. And this is the first flaw. Wouldn't it be better to deliver the FinalResult to the 
// last stage when it is generated without passing it through the whole flow?

// Another flaw of this design is that we, as programmers, have to remember to properly close the flow with andLast after andNext/andNextAsync
// or with .map(_.result) after responseOrNext







