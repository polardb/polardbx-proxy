/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2011-1-23)
 */
package com.alibaba.polardbx.proxy.parser.ast.expression.primary.function;

import com.alibaba.polardbx.proxy.parser.ast.expression.Expression;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.List;

/**
 * 用户自定义函数
 *
 * @author Zeratulll 2016年2月22日 下午8:27:19
 * @since 5.0.0
 */
public class UserDefFunction extends FunctionExpression {

    private final String funcNameUpcase;

    public UserDefFunction(String funcNameUpcase, List<Expression> arguments) {
        super("_USER_DEF", arguments);
        this.funcNameUpcase = funcNameUpcase;
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return new UserDefFunction(null, arguments);
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

    public String getFunctionName() {
        return funcNameUpcase;
    }

}
