/*
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
package io.trino.eventlistener;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class EventListenerConfig
{
    private List<File> eventListenerFiles = ImmutableList.of();
    private int maxConcurrentQueryCompletedEvents = 100;

    @NotNull
    public List<@FileExists File> getEventListenerFiles()
    {
        return eventListenerFiles;
    }

    @Config("event-listener.config-files")
    public EventListenerConfig setEventListenerFiles(List<String> eventListenerFiles)
    {
        this.eventListenerFiles = eventListenerFiles.stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    @Min(1)
    public int getMaxConcurrentQueryCompletedEvents()
    {
        return maxConcurrentQueryCompletedEvents;
    }

    @Config("event-listener.max-concurrent-query-completed-events")
    public EventListenerConfig setMaxConcurrentQueryCompletedEvents(int maxConcurrentQueryCompletedEvents)
    {
        this.maxConcurrentQueryCompletedEvents = maxConcurrentQueryCompletedEvents;
        return this;
    }
}
