// Generated from TypeExpr.g4 by ANTLR 4.7.2

package marmot.type.typeexpr;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link TypeExprParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface TypeExprVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#typeIdExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeIdExpr(TypeExprParser.TypeIdExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#simpleTypeIdExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleTypeIdExpr(TypeExprParser.SimpleTypeIdExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#listTypeIdExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitListTypeIdExpr(TypeExprParser.ListTypeIdExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#recordTypeIdExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRecordTypeIdExpr(TypeExprParser.RecordTypeIdExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#columnTypeIdListExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnTypeIdListExpr(TypeExprParser.ColumnTypeIdListExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#columnTypeIdExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnTypeIdExpr(TypeExprParser.ColumnTypeIdExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#typeNameExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeNameExpr(TypeExprParser.TypeNameExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#simpleTypeNameExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleTypeNameExpr(TypeExprParser.SimpleTypeNameExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#listTypeNameExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitListTypeNameExpr(TypeExprParser.ListTypeNameExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#recordTypeNameExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRecordTypeNameExpr(TypeExprParser.RecordTypeNameExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#columnTypeNameListExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnTypeNameListExpr(TypeExprParser.ColumnTypeNameListExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#columnTypeNameExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnTypeNameExpr(TypeExprParser.ColumnTypeNameExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link TypeExprParser#sridExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSridExpr(TypeExprParser.SridExprContext ctx);
}