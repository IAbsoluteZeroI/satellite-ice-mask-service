<template>
  <div class="dropper">
    <v-responsive>
      <v-stepper
        v-model="state.step"
        :items="['Загрузка TIFF', 'Обработка', 'Готово']"
        :hide-actions="state.availableStep <= state.step"
      >
        <!-- Шаг 1 -->
        <template #item.1>
          <v-file-input
            v-model="state.inputValue"
            clearable
            label="Загрузите TIFF-файл"
            accept=".tif,.tiff"
            @update:model-value="onFileSet"
          />
        </template>

        <!-- Шаг 2 -->
        <template #item.2>
          <div class="progress">
            <v-progress-circular indeterminate color="primary" size="40" />
            <span>Идёт обработка TIFF файла...</span>
          </div>
        </template>

        <!-- Шаг 3 -->
        <template #item.3>
          <div class="map-container">
            <l-map
              ref="mapRef"
              :zoom="5"
              :center="[65, 45]"
              style="height: 600px; width: 100%"
              :useGlobalLeaflet="false"
            >
              <l-tile-layer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
              <l-geo-json
                v-if="state.geojsonData"
                :geojson="state.geojsonData"
                :options="geojsonOptions"
                @ready="onGeoJsonReady"
              />
            </l-map>
            <div class="top-bar">
              <a :href="HOST + 'download'" class="btn" target="_blank">📥 Скачать маску</a>
            </div>
          </div>
        </template>
      </v-stepper>
    </v-responsive>
  </div>
</template>

<script setup lang="ts">
import { reactive, computed, watch, ref } from 'vue';
import axios from 'axios';
import 'leaflet/dist/leaflet.css';
import {
  LMap,
  LTileLayer,
  LGeoJson
} from '@vue-leaflet/vue-leaflet';

const HOST = 'http://' + window.location.hostname + ':8000/';

const state = reactive({
  step: 1,
  availableStep: 1,
  inputValue: null as File | null,
  geojsonData: null as any,
  polling: false,
});

const geojsonOptions = {
  style: {
    color: '#0078d4',
    weight: 2,
    fillOpacity: 0.4,
  },
};

const mapRef = ref<InstanceType<typeof LMap> | null>(null);

const onGeoJsonReady = (layer: any) => {
  if (!layer || !mapRef.value?.leafletObject) return;

  const bounds = layer.getBounds?.();
  if (bounds && bounds.isValid()) {
    mapRef.value.leafletObject.fitBounds(bounds, {
      padding: [20, 20],
      animate: true,
    });
  }
};

const fetchGeojson = async () => {
  try {
    const response = await axios.get(HOST + 'geojson');
    if (response.status === 200) {
      state.geojsonData = response.data;
    }
  } catch (e) {
    console.warn('GeoJSON пока не готов');
  }
};

const pollGeojsonUntilReady = async () => {
  state.polling = true;

  while (state.polling) {
    await fetchGeojson();
    if (state.geojsonData) {
      state.polling = false;
      state.step = 3;
      state.availableStep = 3;
      return;
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
};

const doStep = async () => {
  if (state.step !== 2 || !state.inputValue) return;

  const formData = new FormData();
  formData.append('file', state.inputValue);

  state.geojsonData = null;

  try {
    const response = await axios.post(HOST + 'upload/', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });

    if (response.status === 200) {
      state.geojsonData = response.data;
      state.step = 3;
      state.availableStep = 3;
    } else {
      state.step = 2;
      state.availableStep = 3;
      pollGeojsonUntilReady();
    }
  } catch (e) {
    console.error('Ошибка отправки файла или сервер долго отвечает:', e);
    state.step = 2;
    pollGeojsonUntilReady();
  }
};

const onFileSet = (v: File | File[]) => {
  const value = v as File;
  if (value) {
    state.inputValue = value;
    state.availableStep = 2;
  } else {
    state.availableStep = 1;
  }
};

watch(computed(() => state.step), doStep);
</script>

<style scoped>
.dropper {
  width: 60%;
  max-width: 60%;
  margin: 0 auto;
  padding: 0;
}

.map-container {
  position: relative;
  width: 100%;
}

.top-bar {
  position: absolute;
  top: 10px;
  right: 10px;
  background: white;
  padding: 8px;
  border-radius: 8px;
  z-index: 999;
}

.btn {
  background-color: #0078d4;
  color: white;
  padding: 8px 12px;
  border-radius: 5px;
  text-decoration: none;
  font-weight: 600;
}

.btn:hover {
  background-color: #005fa3;
}

.progress {
  margin-top: 1rem;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 1rem;
}
</style>
