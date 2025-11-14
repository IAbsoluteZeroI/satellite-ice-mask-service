import 'leaflet/dist/leaflet.css'

<template>
  <v-app theme="light">
    <v-main>
      <router-view />
    </v-main>
  </v-app>
</template>

<script lang="ts" setup>
  //
</script>

<style>
.leaflet-control-attribution {
  display: none !important;
}

.leaflet-control-zoom {
  display: none !important;
}
</style>
