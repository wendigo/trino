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

package io.trino.testing.containers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.StartupCheckStrategy;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.Closeable;

import static com.github.dockerjava.api.model.Ports.Binding.bindPort;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.sun.jna.Platform.isARM;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.startupcheck.StartupCheckStrategy.StartupStatus.FAILED;
import static org.testcontainers.containers.startupcheck.StartupCheckStrategy.StartupStatus.SUCCESSFUL;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

public final class TestContainers
{
    // To reuse container set TESTCONTAINERS_REUSE_ENABLE=true environment variable.
    // TESTCONTAINERS_REUSE_ENABLE is an environment variable defined in testcontainers library.
    private static final boolean TESTCONTAINERS_REUSE_ENABLE = parseBoolean(getenv("TESTCONTAINERS_REUSE_ENABLE"));

    // By default we don't want to run emulated docker images due to their performance
    private static final String TRINO_SKIP_ARCH_CHECK_KEY = "TRINO_SKIP_DOCKER_ARCH_CHECK";
    private static final boolean TRINO_SKIP_ARCH_CHECK = "true".equalsIgnoreCase(getenv(TRINO_SKIP_ARCH_CHECK_KEY));

    private TestContainers() {}

    // You should not close the container directly if you want to reuse it.
    // Instead you should close closeable returned by {@link this::startOrReuse}
    public static Closeable startOrReuse(GenericContainer<?> container)
    {
        boolean reuse = TestcontainersConfiguration.getInstance().environmentSupportsReuse();
        checkState(reuse == TESTCONTAINERS_REUSE_ENABLE, "Unable to enable or disable container reuse");

        container.withReuse(TESTCONTAINERS_REUSE_ENABLE);
        container.start();
        if (reuse) {
            return () -> {};
        }
        return container::stop;
    }

    public static String getPathFromClassPathResource(String resourcePath)
    {
        return forClasspathResource(resourcePath)
                // Container fails to mount jar:file:/<host_path>!<resource_path> resources
                // This ensures that JAR resources are being copied out to tmp locations
                // and mounted from there.
                .getResolvedPath();
    }

    public static void exposeFixedPorts(GenericContainer<?> container)
    {
        checkState(System.getenv("CONTINUOUS_INTEGRATION") == null, "" +
                "Exposing fixed ports should not be used in regular test code. This could break parallel test execution. " +
                "This method is supposed to be invoked from local development helpers only e.g. QueryRunner.main(), " +
                "hence it should never run on CI");

        container.withCreateContainerCmdModifier(cmd -> cmd
                .withHostConfig(requireNonNull(cmd.getHostConfig(), "hostConfig is null")
                        .withPortBindings(container.getExposedPorts().stream()
                                .map(exposedPort -> new PortBinding(bindPort(exposedPort), new ExposedPort(exposedPort)))
                                .collect(toImmutableList()))));
    }

    public static GenericContainer<?> verifyImagePlatformMatchesJvmRuntime(GenericContainer<?> container)
    {
        return container.withStartupCheckStrategy(multipleStartupStrategies(container.getStartupCheckStrategy(), new HasCompatibleProcessorArchitecture()));
    }

    public static StartupCheckStrategy multipleStartupStrategies(StartupCheckStrategy... strategies)
    {
        return new StartupCheckStrategy() {
            @Override
            public StartupStatus checkStartupState(DockerClient dockerClient, String contanerId)
            {
                for (StartupCheckStrategy strategy : strategies) {
                    StartupStatus state = strategy.checkStartupState(dockerClient, contanerId);
                    if (state != SUCCESSFUL) {
                        return state; // Failed or not yet known
                    }
                }
                return SUCCESSFUL;
            }
        };
    }

    public static class HasCompatibleProcessorArchitecture
            extends StartupCheckStrategy
    {
        @Override
        public StartupCheckStrategy.StartupStatus checkStartupState(DockerClient dockerClient, String containerId)
        {
            if (TRINO_SKIP_ARCH_CHECK) {
                return SUCCESSFUL;
            }

            String dockerArch = dockerClient.versionCmd().exec().getArch();
            InspectContainerResponse containerInfo = dockerClient.inspectContainerCmd(containerId).exec();
            String containerPlatform = firstNonNull(requireNonNull(containerInfo, "containerInfo is null").getPlatform(), "").toLowerCase(ENGLISH);

            boolean isJavaOnArm = isARM();
            boolean isDockerOnArm = containerPlatform.contains("arm");

            boolean hasIncompatibleRuntime = (isJavaOnArm != isDockerOnArm);

            if (hasIncompatibleRuntime) {
                System.err.printf("""
                        !!! WARNING !!!
                        Detected incompatible Java and Docker image platforms. The performance of running docker image in such scenarios can vary or it won't work at all.
                        In order to override this warning, set environment variable %s to true.
                        Java is running on: %s (%s).
                        Docker platform is: %s.
                        Docker image platform is: %s.%n""", TRINO_SKIP_ARCH_CHECK_KEY, System.getProperty("os.name"), System.getProperty("os.arch"), dockerArch, containerPlatform);
                return FAILED;
            }

            return SUCCESSFUL;
        }
    }
}
