package org.broadinstitute.dsde.workbench.leonardo.monitor

import cats.effect.IO
import cats.implicits._
import fs2._
import fs2.async.mutable.Queue
import org.broadinstitute.dsde.workbench.leonardo.model.Cluster

/**
  * Created by rtitle on 4/2/18.
  */
trait ClusterMonitorQueue {

  def submit(cluster: IO[Cluster]): IO[Unit]

}

object ClusterMonitorQueue {
  def create(): IO[ClusterMonitorQueue] = {

    async.unboundedQueue[IO, Cluster].flatMap { q =>

      def exec: Stream[IO, Unit] = q.dequeue.evalMap(processCluster)

      async.fork(exec.compile.drain) as {
        new ClusterMonitorQueue {
          override def submit(cluster: IO[Cluster]): IO[Unit] = {
            cluster.flatMap(q.enqueue1)
          }
        }
      }

    }

  }

  def processCluster(cluster: Cluster): IO[Unit] = ???
}