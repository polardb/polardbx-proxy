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
 * (created at 2011-1-25)
 */
package com.alibaba.polardbx.proxy.parser.ast.fragment;

import com.alibaba.polardbx.proxy.parser.ast.ASTNode;
import com.alibaba.polardbx.proxy.parser.ast.expression.primary.ParamMarker;
import com.alibaba.polardbx.proxy.parser.visitor.SQLASTVisitor;

/**
 * @author QIU Shuo
 */
public class Limit implements ASTNode {

    /**
     * when it is null, to sql generated must ignore this number
     */
    private final Number offset;
    private final Number size;
    private final ParamMarker offsetP;
    private final ParamMarker sizeP;

    /**
     * 这个变更标识Limit语句是否使用后置式的OFFSET: LIMIT xxx OFFSET xxx, 默认是false
     */
    private boolean isPostpositiveOffset = false;
    ;

    public Limit(Number offset, Number size) {
        if (offset == null) {
            throw new IllegalArgumentException();
        }
        if (size == null) {
            throw new IllegalArgumentException();
        }
        this.offset = offset;
        this.size = size;
        this.offsetP = null;
        this.sizeP = null;
    }

    public Limit(Number offset, ParamMarker sizeP) {
        if (offset == null) {
            throw new IllegalArgumentException();
        }
        if (sizeP == null) {
            throw new IllegalArgumentException();
        }
        this.offset = offset;
        this.size = null;
        this.offsetP = null;
        this.sizeP = sizeP;
    }

    public Limit(ParamMarker offsetP, Number size) {
        if (offsetP == null) {
            throw new IllegalArgumentException();
        }
        if (size == null) {
            throw new IllegalArgumentException();
        }
        this.offset = null;
        this.size = size;
        this.offsetP = offsetP;
        this.sizeP = null;
    }

    public Limit(ParamMarker offsetP, ParamMarker sizeP) {
        if (offsetP == null) {
            throw new IllegalArgumentException();
        }
        if (sizeP == null) {
            throw new IllegalArgumentException();
        }
        this.offset = null;
        this.size = null;
        this.offsetP = offsetP;
        this.sizeP = sizeP;
    }

    /**
     * @return {@link ParamMarker} or {@link Number}
     */
    public Object getOffset() {
        return offset == null ? offsetP : offset;
    }

    /**
     * @return {@link ParamMarker} or {@link Number}
     */
    public Object getSize() {
        return size == null ? sizeP : size;
    }

    public boolean isPostpositiveOffset() {
        return isPostpositiveOffset;
    }

    public void setPostpositiveOffset(boolean isPostpositiveOffset) {
        this.isPostpositiveOffset = isPostpositiveOffset;
    }

    @Override
    public void accept(SQLASTVisitor visitor) {
        visitor.visit(this);
    }

}
