/*
 * Copyright (c) 2020 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.data.schema.annotation;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler.AnnotationCheckContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
 * This visitor is used to get node in schema to it's resolvedProperties.
 * The nodesToResolvedPropertiesMap will be used for schema annotation compatibility check
 *
 * @author Yingjie Bi
 */
public class AnnotationCheckResolvedPropertiesVisitor implements SchemaVisitor
{
  private Map<PathSpec, Pair<AnnotationCheckContext, Map<String, Object>>> _nodeToResolvedPropertiesMap = new HashMap<>();

  @Override
  public void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order)
  {
    if (order == DataSchemaTraverse.Order.POST_ORDER)
    {
      return;
    }

    DataSchema currentSchema = context.getCurrentSchema();
    Map<String, Object> resolvedProperties = currentSchema.getResolvedProperties();
    if (resolvedProperties.isEmpty())
    {
      return;
    }

    RecordDataSchema.Field schemaField = context.getEnclosingField();
    UnionDataSchema.Member unionDataMember = context.getEnclosingUnionMember();

    AnnotationCheckContext annotationCheckContext = new AnnotationCheckContext();

    annotationCheckContext.setDataSchema(currentSchema);
    annotationCheckContext.setSchemaField(schemaField);
    annotationCheckContext.setUnionMember(unionDataMember);
    annotationCheckContext.setPathToSchema(context.getSchemaPathSpec());

    Pair<AnnotationCheckContext, Map<String, Object>> annotationContextAndResolvedProperties =
        new ImmutablePair<>(annotationCheckContext, resolvedProperties);

    _nodeToResolvedPropertiesMap.put(
          new PathSpec(context.getSchemaPathSpec()), annotationContextAndResolvedProperties);
  }

  @Override
  public VisitorContext getInitialVisitorContext() {
    return null;
  }

  @Override
  public SchemaVisitorTraversalResult getSchemaVisitorTraversalResult() {
    return null;
  }

  public Map<PathSpec, Pair<AnnotationCheckContext, Map<String, Object>>>  getNodeToResolvedPropertiesMap()
  {
    return _nodeToResolvedPropertiesMap;
  }
}
