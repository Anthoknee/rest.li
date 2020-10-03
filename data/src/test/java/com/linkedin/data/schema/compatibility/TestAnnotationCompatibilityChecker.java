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

import com.linkedin.data.DataMap;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.PegasusSchemaAnnotationHandlerImpl;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestAnnotationCompatibilityChecker
{
  @Test(dataProvider = "annotationCompatibilityCheckTestData")
  public void testCheckCompatibility(Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext,
      Map<String, Object>>> olderResolvedPropertiesMap, Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext,
      Map<String, Object>>> newerResolvedPropertiesMap, List<SchemaAnnotationHandler> handlers, List<CompatibilityMessage> expectedCompatibilityMessages)
  {
    List<SchemaAnnotationHandler.AnnotationCompatibilityResult> results = AnnotationCompatibilityChecker
        .getCompatibilityResult(olderResolvedPropertiesMap, newerResolvedPropertiesMap, handlers);
    Assert.assertEquals(results.size(), 1);
    Assert.assertEquals(results.get(0).getMessages().size(), expectedCompatibilityMessages.size());
    List<CompatibilityMessage> actualCompatibilityMessage = (List<CompatibilityMessage>) results.get(0).getMessages();
    for (int i = 0; i < actualCompatibilityMessage.size(); i++)
    {
      Assert.assertEquals(actualCompatibilityMessage.get(i).toString(), expectedCompatibilityMessages.get(i).toString());
    }
  }

  @DataProvider
  private Object[][] annotationCompatibilityCheckTestData()
  {
    ArrayDeque<String> path = new ArrayDeque<>();
    path.add("TestSchema");
    path.add("field1");
    ArrayDeque<String> path2 = new ArrayDeque<>();
    path2.add("TestSchema");
    path2.add("field2");

    String annotationNamespace = "bar";
    String annotationFieldName = "foo";
    Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext,
        Map<String, Object>>> olderResolvedPropertiesMap = new HashMap<>();
    PathSpec olderPathSpec = new PathSpec(path);
    PathSpec olderPathSpec2 = new PathSpec(path2);
    SchemaAnnotationHandler.AnnotationCheckContext olderCheckContext = new SchemaAnnotationHandler.AnnotationCheckContext();
    olderCheckContext.setPathToSchema(path);
    Map<String, Object> olderResolvedProperties = new HashMap<>();
    DataMap olderAnnotationField = new DataMap();
    olderAnnotationField.put(annotationFieldName, 1);
    olderResolvedProperties.put(annotationNamespace, olderAnnotationField);
    Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>> olderAnnotationContextAndResolvedProperties =
        new ImmutablePair<>(olderCheckContext, olderResolvedProperties);
    olderResolvedPropertiesMap.put(olderPathSpec, olderAnnotationContextAndResolvedProperties);

    SchemaAnnotationHandler.AnnotationCheckContext olderCheckContext2 = new SchemaAnnotationHandler.AnnotationCheckContext();
    olderCheckContext2.setPathToSchema(path2);
    Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>> olderAnnotationContextAndResolvedProperties2 =
        new ImmutablePair<>(olderCheckContext2, olderResolvedProperties);
    olderResolvedPropertiesMap.put(olderPathSpec2, olderAnnotationContextAndResolvedProperties2);

    Map<PathSpec, Pair<SchemaAnnotationHandler.AnnotationCheckContext,
        Map<String, Object>>> newerResolvedPropertiesMap = new HashMap<>();
    PathSpec newerPathSpec = new PathSpec(path);
    SchemaAnnotationHandler.AnnotationCheckContext newerCheckContext = new SchemaAnnotationHandler.AnnotationCheckContext();
    newerCheckContext.setPathToSchema(path);
    Map<String, Object> newerResolvedProperties = new HashMap<>();
    DataMap newerAnnotationField = new DataMap();
    newerAnnotationField.put(annotationFieldName, 2);
    newerResolvedProperties.put(annotationNamespace, newerAnnotationField);
    Pair<SchemaAnnotationHandler.AnnotationCheckContext, Map<String, Object>> newerAnnotationContextAndResolvedProperties =
        new ImmutablePair<>(newerCheckContext, newerResolvedProperties);
    newerResolvedPropertiesMap.put(newerPathSpec, newerAnnotationContextAndResolvedProperties);

    CompatibilityMessage expectedMessage = new CompatibilityMessage(olderCheckContext.getPathToSchema(),
        CompatibilityMessage.Impact.ANNOTATION_COMPATIBLE_CHANGE, "Updating annotation field \"%s\" value is backward compatible change", annotationFieldName);

    SchemaAnnotationHandler testHandler = new PegasusSchemaAnnotationHandlerImpl(annotationNamespace)
    {
      @Override
      public String getAnnotationNamespace()
      {
        return annotationNamespace;
      }

      @Override
      public AnnotationCompatibilityResult annotationCompatibilityCheck(Map<String,Object> olderResolvedProperties, Map<String, Object> newerResolvedProperties,
          AnnotationCheckContext olderContext, AnnotationCheckContext newerContext)
      {
        AnnotationCompatibilityResult result = new AnnotationCompatibilityResult();

        if (olderResolvedProperties.get(annotationNamespace) == null)
        {
          result.getMessages().add(new CompatibilityMessage(olderContext.getPathToSchema(),
              CompatibilityMessage.Impact.ANNOTATION_INCOMPATIBLE_CHANGE, "Adding new annotation \"%s\" is backward incompatible change", annotationNamespace));
        }
        else if (newerResolvedProperties.get(annotationNamespace) == null)
        {
          result.getMessages().add(new CompatibilityMessage(olderContext.getPathToSchema(),
              CompatibilityMessage.Impact.ANNOTATION_INCOMPATIBLE_CHANGE, "Deleting existed annotation \"%s\" is backward incompatible change", annotationNamespace));
        }
        else
        {
          DataMap older = (DataMap) olderResolvedProperties.get(annotationNamespace);
          DataMap newer = (DataMap) newerResolvedProperties.get(annotationNamespace);
          if (older.containsKey(annotationFieldName) && !newer.containsKey(annotationFieldName))
          {
            result.getMessages().add(new CompatibilityMessage(olderContext.getPathToSchema(),
                CompatibilityMessage.Impact.ANNOTATION_INCOMPATIBLE_CHANGE, "remove annotation field \"%s\" is backward incompatible change", annotationFieldName));
          }
          if (older.containsKey("foo") && newer.containsKey("foo"))
          {
            if (older.get("foo") != newer.get("foo"))
            {
              result.getMessages().add(new CompatibilityMessage(olderContext.getPathToSchema(),
                  CompatibilityMessage.Impact.ANNOTATION_COMPATIBLE_CHANGE, "Updating annotation field \"%s\" value is backward compatible change", annotationFieldName));
            }
          }
        }
        return result;
      }
    };
    return new Object[][]
        {
            {
                olderResolvedPropertiesMap,
                newerResolvedPropertiesMap,
                Collections.singletonList(testHandler),
                Collections.singletonList(expectedMessage)
            }
        };
  }
}