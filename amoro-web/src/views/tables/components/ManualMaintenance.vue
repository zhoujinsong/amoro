<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
/ -->

<script lang="ts">
import { defineComponent, reactive, ref } from 'vue'
import { message } from 'ant-design-vue'
import { useRoute } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { triggerOptimizing } from '@/services/table.service'

export default defineComponent({
  name: 'ManualMaintenance',
  setup() {
    const { t } = useI18n()
    const route = useRoute()
    const query = route.query

    const sourceData = reactive({
      catalog: '',
      db: '',
      table: '',
      ...query,
    })

    const loading = ref<boolean>(false)
    const confirmVisible = ref<boolean>(false)

    function handleTriggerOptimizing() {
      confirmVisible.value = false
      loading.value = true
      triggerOptimizing(sourceData)
        .then(() => {
          message.success(t('triggerOptimizingSuccess'))
        })
        .catch((error: any) => {
          message.error(error?.message || t('triggerOptimizingFailed'))
        })
        .finally(() => {
          loading.value = false
        })
    }

    function showConfirm() {
      confirmVisible.value = true
    }

    function cancelConfirm() {
      confirmVisible.value = false
    }

    return {
      t,
      sourceData,
      loading,
      confirmVisible,
      handleTriggerOptimizing,
      showConfirm,
      cancelConfirm,
    }
  },
})
</script>

<template>
  <div class="manual-maintenance">
    <div class="maintenance-section">
      <h3>{{ t('optimizingOperations') }}</h3>
      <div class="operation-card">
        <div class="operation-info">
          <h4>{{ t('triggerOptimizing') }}</h4>
          <p>{{ t('triggerOptimizingDesc') }}</p>
        </div>
        <a-button
          type="primary"
          :loading="loading"
          @click="showConfirm"
        >
          {{ t('triggerNow') }}
        </a-button>
      </div>
    </div>

    <a-modal
      v-model:open="confirmVisible"
      :title="t('confirmTriggerOptimizing')"
      :ok-button-props="{ style: { display: 'none' } }"
      :cancel-button-props="{ style: { display: 'none' } }"
    >
      <p>{{ t('triggerOptimizingConfirmText') }}</p>
      <template #footer>
        <a-button @click="cancelConfirm">{{ t('cancel') }}</a-button>
        <a-button
          type="primary"
          :loading="loading"
          @click="handleTriggerOptimizing"
        >
          {{ t('confirm') }}
        </a-button>
      </template>
    </a-modal>
  </div>
</template>

<style lang="less" scoped>
.manual-maintenance {
  padding: 24px;

  .maintenance-section {
    margin-bottom: 32px;

    h3 {
      font-size: 18px;
      font-weight: bold;
      color: #102048;
      margin-bottom: 16px;
    }

    .operation-card {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 16px;
      border: 1px solid #e8e8f0;
      border-radius: 8px;
      background: #fff;

      .operation-info {
        flex: 1;

        h4 {
          font-size: 16px;
          font-weight: bold;
          color: #102048;
          margin-bottom: 8px;
        }

        p {
          font-size: 14px;
          color: #666;
          margin: 0;
        }
      }
    }
  }
}
</style>
