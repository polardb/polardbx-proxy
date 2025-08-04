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
package com.alibaba.polardbx.proxy.parser.ast.stmt.json;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.fragment.json.AbstractPathLeg;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

import java.util.List;

/**
 * @author arnkore 2017-07-12 16:39
 */
public class JsonPathExprStatement implements ASTNode {

    private List<AbstractPathLeg> pathLegs;

    public JsonPathExprStatement(List<AbstractPathLeg> pathLegs) {
        this.pathLegs = pathLegs;
    }

    @Override
    public String toString() {
        StringBuilder appendable = new StringBuilder();
        appendable.append("$");

        for (AbstractPathLeg pathLeg : pathLegs) {
            appendable.append(pathLeg.toString());
        }

        return appendable.toString();
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
//        visitor.visit(this);
    }

    public List<AbstractPathLeg> getPathLegs() {
        return pathLegs;
    }

    public void setPathLegs(List<AbstractPathLeg> pathLegs) {
        this.pathLegs = pathLegs;
    }
}
