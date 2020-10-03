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
package com.linkedin.data.schema.compatibility;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser;
import com.linkedin.data.schema.annotation.AnnotationCheckResolvedPropertiesVisitor;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler.AnnotationCompatibilityResult;
import com.linkedin.data.schema.annotation.SchemaAnnotationProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Schema Annotation compatibility checker
 *  The annotation compatibility check is part of the annotation processing framework.
 *  If users use annotation processor to resolve properties,
 *  they may also provide a annotation compatibility method to define the way to check annotation's compatibility.
 *  In this checker, it will call the annotationCompatibilityCheck method which provide in the SchemaAnnotationHandler
 *  by user to do the annotation compatibility check.
 *
 * @author Yingjie Bi
 */
public class AnnotationCompatibilityChecker
{
  private static final Logger _log = LoggerFactory.getLogger(AnnotationCompatibilityChecker.class);

  /**
   * Check the annotation compatibilities
   * process olderSchema and newerSchema in the SchemaAnnotationProcessor to get the resolved result with resolvedProperties.
   * then using the resolvedProperties to do the annotation compatibility check.
   * @param olderSchema
   * @param newerSchema
   * @param handlers SchemaAnnotationHandler list
   * @return List<AnnotationCompatibilityResult>
   */
  public static List<AnnotationCompatibilityResult> checkPegasusSchemaCompatibility(DataSchema olderSchema, DataSchema newerSchema,
      List<SchemaAnnotationHandler> handlers)
  {
    SchemaAnnotationProcessor.SchemaAnnotationProcessResult olderSchemaResult = processSchemaAnnotation(olderSchema, handlers);
    SchemaAnnotationProcessor.SchemaAnnotationProcessResult newerSchemaResult = processSchemaAnnotation(newerSchema, handlers);
    Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>>> olderResolvedPropertiesMap
        = getNodeToResolvedProperties(olderSchemaResult);
    Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>>> newerResolvedPropertiesMap
        = getNodeToResolvedProperties(newerSchemaResult);

    return getCompatibilityResult(olderResolvedPropertiesMap, newerResolvedPropertiesMap, handlers);
  }

  /**
   * Iterate the nodeToResolverPropertiesMap, if olderResolvedPropertiesMap and newerResolvedPropertiesMap contain same pathSpec,
   * calling annotationCompatibilityCheck api which is provided in the SchemaAnnotationHandler to do the annotation compatibility check.
   * @param olderResolvedPropertiesMap
   * @param newerResolvedPropertiesMap
   * @param handlers SchemaAnnotationHandler list
   * @return List<AnnotationCompatibilityResult>
   */
  static List<AnnotationCompatibilityResult> getCompatibilityResult(Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext,
      Map<String, Object>>> olderResolvedPropertiesMap, Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext,
      Map<String, Object>>> newerResolvedPropertiesMap, List<SchemaAnnotationHandler> handlers)
  {
    List<AnnotationCompatibilityResult> results = new ArrayList<>();
    for (SchemaAnnotationHandler handler: handlers)
    {
      olderResolvedPropertiesMap.entrySet().stream().forEach(e ->
          {
            String annotationNamespace = handler.getAnnotationNamespace();
            PathSpec pathSpec = e.getKey();
            if (newerResolvedPropertiesMap.containsKey(pathSpec))
            {
              Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>> olderCheckContextAndResolvedProperty
                  = olderResolvedPropertiesMap.get(pathSpec);
              Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>> newerCheckContextAndResolvedProperty
                  = newerResolvedPropertiesMap.get(pathSpec);

              Map<String, Object> olderResolvedProperties = olderCheckContextAndResolvedProperty.getValue();
              Map<String, Object> newerResolvedProperties = newerCheckContextAndResolvedProperty.getValue();

              if (olderResolvedProperties.containsKey(annotationNamespace) || newerResolvedProperties.containsKey(annotationNamespace))
              {
                AnnotationCompatibilityResult result = handler.annotationCompatibilityCheck(olderResolvedProperties,
                    newerResolvedProperties, olderCheckContextAndResolvedProperty.getKey(), newerCheckContextAndResolvedProperty.getKey());
                results.add(result);
              }
            }
          });
    }
    return results;
  }

  private static Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>>> getNodeToResolvedProperties(
      SchemaAnnotationProcessor.SchemaAnnotationProcessResult result)
  {
    AnnotationCheckResolvedPropertiesVisitor visitor = new AnnotationCheckResolvedPropertiesVisitor();
    DataSchemaRichContextTraverser traverser = new DataSchemaRichContextTraverser(visitor);
    traverser.traverse(result.getResultSchema());
    return visitor.getNodeToResolvedPropertiesMap();
  }

  private static SchemaAnnotationProcessor.SchemaAnnotationProcessResult processSchemaAnnotation(DataSchema dataSchema,
      List<SchemaAnnotationHandler> handlers)
  {
    SchemaAnnotationProcessor.SchemaAnnotationProcessResult result =
        SchemaAnnotationProcessor.process(handlers, dataSchema, new SchemaAnnotationProcessor.AnnotationProcessOption());
    // If any of the nameDataSchema failed to be processed, throw exception
    if (result.hasError())
    {
      String schemaName = ((NamedDataSchema) dataSchema).getFullName();
      throw new RuntimeException("Annotation processing for data schema：" + schemaName + " failed, detailed error: "+ result.getErrorMsgs());
    }
    return result;
  }
}
