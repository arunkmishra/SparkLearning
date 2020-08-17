package com.arun.utils
import org.apache.spark.sql.catalyst.expressions.{Add, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object CustomOptimizer {

  object AdditionOptimizerRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
      case Add(left, right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 0.0 =>
        println("add optimization implemented")
        left
    }
  }

}


