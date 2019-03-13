package com.project.app


//Main object to start streaming job
object Main extends App{
  val c= new ProcessStreams(args(0))
  val context = c.createContext()
  val d = c.processStream(context._2,context._3)

}