#pragma once
#include "execution/compiler/expression/expression_translator.h"

namespace tpl::compiler {

class ConjunctionTranslator : public ExpressionTranslator {

 public:
  ConjunctionTranslator(const terrier::parser::AbstractExpression *expression, CompilationContext *context);

  ast::Expr *DeriveExpr(const terrier::parser::AbstractExpression *expression, RowBatch *row) override;
};
}